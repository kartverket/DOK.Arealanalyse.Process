import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple
from uuid import UUID, NAMESPACE_URL, uuid5
import yaml
import structlog
from structlog.stdlib import BoundLogger
from xmlschema import XMLSchema
from .xml_schema import compile_xml_schema
from .dok_status import get_dok_status
from ..caching.xsd import hash_url
from ..models.config import DatasetConfig, QualityConfig, QualityIndicator
from ..utils.helpers.common import get_config_file_paths
from ..utils.helpers.filter import compile_cql_filter
from ..constants import USE_XML_SCHEMAS

# How often the config directory is checked for changes
_CHECK_INTERVAL_SECONDS = 10

_logger: BoundLogger = structlog.get_logger(__name__)


@dataclass
class _ConfigSnapshot:
    fingerprint: Tuple[Tuple[str, int, int], ...]
    datasets: List[DatasetConfig] = field(default_factory=list)
    datasets_by_id: Dict[UUID, DatasetConfig] = field(default_factory=dict)
    common_indicators: List[QualityIndicator] = field(default_factory=list)
    indicators_by_id: Dict[UUID, List[QualityIndicator]] = field(default_factory=dict)


_snapshot: _ConfigSnapshot | None = None
_last_checked: float = 0
_reload_lock = asyncio.Lock()


async def get_dataset_configs() -> List[DatasetConfig]:
    snapshot = await _get_snapshot()

    return snapshot.datasets


async def get_dataset_config(config_id: UUID) -> DatasetConfig | None:
    snapshot = await _get_snapshot()

    return snapshot.datasets_by_id.get(config_id)


async def get_quality_indicator_configs(config_id: UUID) -> List[QualityIndicator]:
    snapshot = await _get_snapshot()

    return snapshot.common_indicators + snapshot.indicators_by_id.get(config_id, [])


async def get_not_implemented_dataset_configs() -> List[DatasetConfig]:
    """Returns configs for the DOK datasets (from the DOK status register)
    that have no dataset configuration."""
    snapshot = await _get_snapshot()
    metadata_ids = {str(config.metadata_id)
                    for config in snapshot.datasets if config.metadata_id is not None}
    configs: List[DatasetConfig] = []

    for dataset in await get_dok_status():
        dataset_id: str = dataset['dataset_id']

        if dataset_id in metadata_ids:
            continue

        theme: str = dataset.get('theme') or ''

        configs.append(DatasetConfig.model_construct(
            config_id=uuid5(NAMESPACE_URL, f'dokanalyse:not-implemented:{dataset_id}'),
            name=(dataset.get('name') or dataset_id).replace('-', '_'),
            metadata_id=dataset_id,
            themes=[theme] if theme else []
        ))

    return configs


async def _get_snapshot() -> _ConfigSnapshot:
    global _snapshot, _last_checked

    if _snapshot is not None and time.monotonic() - _last_checked < _CHECK_INTERVAL_SECONDS:
        return _snapshot

    async with _reload_lock:
        if _snapshot is not None and time.monotonic() - _last_checked < _CHECK_INTERVAL_SECONDS:
            return _snapshot

        fingerprint = await asyncio.to_thread(_get_fingerprint)

        if _snapshot is None or _snapshot.fingerprint != fingerprint:
            _snapshot = await _load_snapshot(fingerprint)

        _last_checked = time.monotonic()

        return _snapshot


async def _load_snapshot(fingerprint: Tuple[Tuple[str, int, int], ...]) -> _ConfigSnapshot:
    snapshot = _ConfigSnapshot(fingerprint)
    documents = await asyncio.to_thread(_read_documents, [Path(entry[0]) for entry in fingerprint])

    for path, data in documents:
        match data.get('type'):
            case 'dataset':
                config = await _create_dataset_config(path, data)

                if config is None:
                    continue

                if config.config_id in snapshot.datasets_by_id:
                    _logger.error('Duplicate dataset config_id, the config is ignored',
                                  config_id=str(config.config_id), file=path.name)
                    continue

                snapshot.datasets.append(config)
                snapshot.datasets_by_id[config.config_id] = config
            case 'quality':
                quality_config = await _create_quality_config(path, data)

                if quality_config is None:
                    continue

                indicators = [indicator for indicator in quality_config.indicators if not indicator.disabled]

                if quality_config.config_id is None:
                    snapshot.common_indicators.extend(indicators)
                else:
                    snapshot.indicators_by_id.setdefault(quality_config.config_id, []).extend(indicators)
            case other:
                _logger.error('Unknown config type', type=other, file=path.name)

    if not snapshot.datasets:
        raise RuntimeError(
            'Could not create any dataset configurations from the files in "DOKANALYSE_DATASETS_CONFIG_DIR"')

    _logger.info('Loaded dataset configurations', count=len(snapshot.datasets))

    return snapshot


def _read_documents(paths: List[Path]) -> List[Tuple[Path, Dict[str, Any]]]:
    documents: List[Tuple[Path, Dict[str, Any]]] = []

    for path in paths:
        try:
            for data in yaml.safe_load_all(path.read_text(encoding='utf-8')):
                if not isinstance(data, dict) or data.get('disabled'):
                    continue

                documents.append((path, data))
        except (OSError, yaml.YAMLError) as err:
            _logger.error('Reading config file failed', file=path.name, error=str(err))

    return documents


async def _create_dataset_config(path: Path, data: Dict[str, Any]) -> DatasetConfig | None:
    try:
        config = DatasetConfig(**data)

        if config.wfs:
            for layer in config.layers:
                if layer.filter:
                    layer.filter_func = compile_cql_filter(layer.filter)

            if USE_XML_SCHEMAS:
                config.xml_schema = await _compile_wfs_xml_schema(
                    config.get_feature_service_url())

        return config
    except Exception as err:
        # Validation errors and invalid CQL filters
        _logger.error('Dataset config creation failed', file=path.name,
                      config_id=data.get('config_id'), error=str(err))
        return None


async def _create_quality_config(path: Path, data: Dict[str, Any]) -> QualityConfig | None:
    try:
        config = QualityConfig(**data)

        for indicator in config.indicators:
            if indicator.wfs and USE_XML_SCHEMAS:
                indicator.wfs.xml_schema = await _compile_wfs_xml_schema(
                    str(indicator.wfs.url))

            if indicator.input_filter:
                indicator.input_filter_func = compile_cql_filter(
                    indicator.input_filter)

        return config
    except Exception as err:
        _logger.error('Quality config creation failed', file=path.name,
                      config_id=data.get('config_id'), error=str(err))
        return None


async def _compile_wfs_xml_schema(wfs_url: str) -> XMLSchema | None:
    return await compile_xml_schema(hash_url(wfs_url), wfs_url)


def _get_fingerprint() -> Tuple[Tuple[str, int, int], ...]:
    entries = []

    for path in sorted(get_config_file_paths()):
        try:
            stat = path.stat()
        except OSError:
            continue

        entries.append((str(path.resolve()), stat.st_mtime_ns, stat.st_size))

    return tuple(entries)


__all__ = [
    'get_dataset_configs',
    'get_dataset_config',
    'get_quality_indicator_configs',
    'get_not_implemented_dataset_configs'
]
