import json
import yaml
import hashlib
from uuid import UUID
from pathlib import Path
from async_lru import alru_cache
from typing import Any, Callable, Dict, List, Tuple
import structlog
from structlog.stdlib import BoundLogger
from pydantic import ValidationError
from pygeofilter.parsers.cql2_text import parse
from pygeofilter.backends.native.evaluate import NativeEvaluator
from xmlschema import XMLSchema
from .xml_schema import compile_xml_schema
from ..caching.not_implemented_datasets import get_or_create_not_implemented_datasets
from ..models.config import DatasetConfig, QualityConfig, QualityIndicator
from ..utils.helpers.common import get_config_file_paths
from ..utils.http_context import get_session
from ..constants import USE_XML_SCHEMAS

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_dataset_configs() -> List[DatasetConfig]:
    dataset_configs, _ = await _load_configs()

    return dataset_configs


async def get_dataset_config(config_id: UUID) -> DatasetConfig | None:
    dataset_configs, _ = await _load_configs()

    config = next(
        (conf for conf in dataset_configs if conf.config_id == config_id), None)

    return config


async def get_quality_indicator_configs(config_id: UUID) -> List[QualityIndicator]:
    _, quality_configs = await _load_configs()
    indicators: List[QualityIndicator] = []

    for config in quality_configs:
        id: UUID | None = config.config_id

        if not id or id == config_id:
            for indicator in config.indicators:
                if not indicator.disabled:
                    indicators.append(indicator)

    return indicators


async def get_not_implemented_dataset_configs() -> List[DatasetConfig]:
    dataset_configs, _ = await _load_configs()
    metadata_ids = [str(config.metadata_id)
                    for config in dataset_configs if config.metadata_id is not None]
    datasets = await _get_not_implemented_datasets(metadata_ids)
    configs: List[DatasetConfig] = []

    for dataset in datasets:
        config = await _create_dataset_config(dataset)

        if config:
            configs.append(config)

    return configs


async def _load_configs() -> Tuple[List[DatasetConfig], List[QualityConfig]]:
    paths = get_config_file_paths()
    dataset_configs: List[DatasetConfig] = []
    quality_configs: List[QualityConfig] = []

    for path in paths:
        dataset_config, quality_config = await _load_config(path)

        if dataset_config:
            dataset_configs.append(dataset_config)

        if quality_config:
            quality_configs.append(quality_config)

    if len(dataset_configs) == 0:
        raise Exception(
            'Could not create any dataset configurations from the files in "DOKANALYSE_CONFIG_DIR"')

    return dataset_configs, quality_configs


async def _load_config(path: Path) -> Tuple[DatasetConfig | None, QualityConfig | None]:
    mtime_ns, size = _get_fingerprint(path)

    return await _load_cached_config(str(path.resolve()), mtime_ns, size)


@alru_cache(maxsize=4096)
async def _load_cached_config(path_str: str, mtime_ns: int, size: int) -> Tuple[DatasetConfig | None, QualityConfig | None]:
    path = Path(path_str)
    results = yaml.safe_load_all(path.read_text(encoding='utf-8'))
    result: Dict[str, Any]
    dataset_config = None
    quality_config = None

    for result in results:
        if not result or result.get('disabled'):
            continue

        type = result.get('type')

        if type == 'dataset':
            config = await _create_dataset_config(result)

            if config is not None:
                dataset_config = config
        elif type == 'quality':
            config = await _create_quality_config(result)

            if config is not None:
                quality_config = config

    return dataset_config, quality_config


async def _create_dataset_config(data: Dict) -> DatasetConfig | None:
    try:
        config = DatasetConfig(**data)

        if config.disabled:
            return None

        if config.wfs:
            layers = [layer for layer in config.layers if layer.filter]

            for layer in layers:
                layer.filter_func = _compile_cql_filter(layer.filter)

            if USE_XML_SCHEMAS:
                config.xml_schema = await _compile_wfs_xml_schema(
                    config.get_feature_service_url())

        return config
    except ValidationError as err:
        _logger.error('Dataset config creation failed', error=str(err))
        return None


async def _create_quality_config(data: Dict) -> QualityConfig | None:
    try:
        config = QualityConfig(**data)

        for indicator in config.indicators:
            if indicator.wfs and USE_XML_SCHEMAS:
                indicator.wfs.xml_schema = await _compile_wfs_xml_schema(
                    str(indicator.wfs.url))

            if indicator.input_filter:
                indicator.input_filter_func = _compile_cql_filter(
                    indicator.input_filter)

        return config
    except ValidationError as err:
        _logger.error('Quality config creation failed', error=str(err))
        return None


def _compile_cql_filter(cql_text: str) -> Callable[[Dict], bool]:
    ast: Any = parse(cql_text)
    predicate = NativeEvaluator(use_getattr=False).evaluate(ast)

    return predicate


async def _compile_wfs_xml_schema(wfs_url: str) -> XMLSchema | None:
    id = _hash_url(wfs_url)
    schema = await compile_xml_schema(id, wfs_url)

    return schema


async def _get_not_implemented_datasets(metadata_ids: List[str]) -> List[Dict[str, Any]]:
    try:
        path = await get_or_create_not_implemented_datasets(metadata_ids, get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error(
            'Getting not implemented datasets failed', error=str(err))
        return []


def _get_fingerprint(path: Path) -> Tuple[int, int]:
    st = path.stat()
    return st.st_mtime_ns, st.st_size


def _hash_url(url: str) -> str:
    url_bytes = url.encode('utf-8')
    hash_object = hashlib.sha256(url_bytes)
    hex_digest = hash_object.hexdigest()

    return hex_digest


__all__ = [
    'get_dataset_configs',
    'get_dataset_config',
    'get_quality_indicator_configs',
    'get_not_implemented_dataset_configs'
]
