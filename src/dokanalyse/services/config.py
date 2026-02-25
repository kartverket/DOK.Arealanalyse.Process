import json
import yaml
import hashlib
from pathlib import Path
from uuid import UUID, uuid4
from async_lru import alru_cache
from typing import Dict, List, Tuple, Any
import structlog
from structlog.stdlib import BoundLogger
from pydantic import ValidationError
from pygeofilter.parsers.cql2_text import parse
from pygeofilter.backends.native.evaluate import NativeEvaluator
from xmlschema import XMLSchema
from .dok_status import get_dok_status
from ..models.exceptions import DokAnalysisException
from ..models.config import DatasetConfig, QualityConfig, QualityIndicator, Layer, FeatureService
from ..utils.helpers.common import get_env_var, should_refresh_cache
from ..constants import CACHE_DIR
from .xml_schema import compile_xml_schema


_LOGGER: BoundLogger = structlog.get_logger(__name__)
_NOT_IMPLEMENTED_DATASETS_CACHE_DAYS = 2


async def get_dataset_configs() -> List[DatasetConfig]:
    dataset_configs, _ = await _load_configs()

    return dataset_configs


async def get_dataset_config(config_id: UUID) -> DatasetConfig:
    dataset_configs, _ = await _load_configs()

    config = next(
        (conf for conf in dataset_configs if conf.config_id == config_id), None)

    return config


async def get_quality_indicator_configs(config_id: UUID) -> List[QualityIndicator]:
    _, quality_configs = await _load_configs()
    indicators: List[QualityIndicator] = []

    for config in quality_configs:
        id: UUID = config.config_id

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
        configs.append(await _create_dataset_config(dataset))

    return configs


async def _load_configs() -> Tuple[List[DatasetConfig], List[QualityConfig]]:
    config_dir = get_env_var('DOKANALYSE_DATASETS_CONFIG_DIR')

    if config_dir is None:
        raise DokAnalysisException(
            'The environment variable "DOKANALYSE_DATASETS_CONFIG_DIR" is not set')

    path = Path(config_dir)

    if not path.is_dir():
        raise DokAnalysisException(
            f'The "DOKANALYSE_DATASETS_CONFIG_DIR" path ({path}) is not a directory')

    glob = path.glob('*.yml')
    paths = [path for path in glob if path.is_file()]

    dataset_configs: List[DatasetConfig] = []
    quality_configs: List[QualityConfig] = []

    for path in paths:
        dataset_config, quality_config = await _load_config(path)

        if dataset_config:
            dataset_configs.append(dataset_config)

        if quality_config:
            quality_configs.append(quality_config)

    if len(dataset_configs) == 0:
        raise DokAnalysisException(
            f'Could not create any dataset configurations from the files in "DOKANALYSE_CONFIG_DIR"')

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


async def _create_dataset_config(data: Dict) -> DatasetConfig:
    try:
        config = DatasetConfig(**data)

        if config.wfs:
            _compile_wfs_filters(config.layers)
            config.xml_schema = await _compile_wfs_xml_schema(config.get_feature_service_url())

        return config if not config.disabled else None
    except ValidationError as err:
        _LOGGER.error('Dataset config creation failed', error=str(err))
        return None


async def _create_quality_config(data: Dict) -> QualityConfig:
    try:
        config = QualityConfig(**data)
        coverage_services = [indicator.wfs for indicator in config.indicators if indicator.wfs]
        
        for wfs in coverage_services:
            wfs.xml_schema = await _compile_wfs_xml_schema(str(wfs.url))

        return config
    except ValidationError as err:
        _LOGGER.error('Quality config creation failed', error=str(err))
        return None


def _compile_wfs_filters(layers: List[Layer]) -> None:
    for layer in layers:
        if layer.filter:
            ast: Any = parse(layer.filter)
            layer.filter_func = NativeEvaluator(
                use_getattr=False).evaluate(ast)


async def _compile_wfs_xml_schema(wfs_url: str) -> XMLSchema | None:
    id = _hash_url(wfs_url)
    url = f'{wfs_url}?service=WFS&version=2.0.0&request=DescribeFeatureType'
    schema = await compile_xml_schema(id, url)

    return schema


async def _get_not_implemented_datasets(metadata_ids: List[str]) -> List[Dict]:
    file_path = Path(CACHE_DIR).joinpath('not-implemented-datasets.json')

    if not file_path.exists() or should_refresh_cache(file_path, _NOT_IMPLEMENTED_DATASETS_CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        dok_status_all = await get_dok_status()
        configs: List[Dict] = []

        for dok_status in dok_status_all:
            dataset_id: str = dok_status['dataset_id']

            if dataset_id in metadata_ids:
                continue

            theme: str = dok_status.get('theme')

            configs.append({
                'config_id': str(uuid4()),
                'metadata_id': dataset_id,
                'themes': [theme] if theme else []
            })

        json_object = json.dumps(configs, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return configs
    else:
        with file_path.open(encoding='utf-8') as file:
            configs = json.load(file)

        return configs


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
