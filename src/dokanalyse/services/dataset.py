import json
from pathlib import Path
from uuid import UUID
from typing import Any, Dict, List, Literal
import structlog
from structlog.stdlib import BoundLogger
from .config import get_dataset_configs
from .caching import cache_file, should_refresh_cache
from ..models.config import DatasetConfig
from ..utils.event_loop_manager import get_session, get_semaphore
from ..constants import CACHE_DIR

_API_BASE_URL = 'https://register.geonorge.no/api/det-offentlige-kartgrunnlaget-kommunalt.json?municipality='
_CACHE_DAYS = 7

_logger: BoundLogger = structlog.get_logger(__name__)


def get_dataset_type(config: DatasetConfig) -> Literal['wfs', 'arcgis', 'ogc_api'] | None:
    if config.wfs is not None:
        return 'wfs'
    elif config.arcgis is not None:
        return 'arcgis'
    elif config.ogc_api is not None:
        return 'ogc_api'

    return None


async def get_config_ids(data: Dict[str, Any], municipality_number: str) -> Dict[UUID, bool]:
    include_chosen_dok: bool = data.get('includeFilterChosenDOK', True)
    kartgrunnlag = await _get_kartgrunnlag(municipality_number) if include_chosen_dok else []
    configs = await _get_datasets_by_theme(data.get('theme'))

    datasets: Dict[UUID, bool] = {}

    for config in configs:
        if include_chosen_dok:
            datasets[config.config_id] = str(
                config.metadata_id) in kartgrunnlag
        else:
            datasets[config.config_id] = True

    return datasets


async def _get_datasets_by_theme(theme: str) -> List[DatasetConfig]:
    dataset_configs = await get_dataset_configs()
    configs: List[DatasetConfig] = []

    for config in dataset_configs:
        themes = list(map(lambda theme: theme.lower(), config.themes))

        if theme is None or theme.lower() in themes:
            configs.append(config)

    return configs


async def _get_kartgrunnlag(municipality_number: str) -> List[str]:
    if municipality_number is None:
        return []

    file_path = Path(CACHE_DIR).joinpath(
        'dok-datasets').joinpath(f'{municipality_number}.json')

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        try:
            async def producer() -> str:
                dataset_ids = await _fetch_dataset_ids(municipality_number)
                json_str = json.dumps(
                    dataset_ids, indent=2, ensure_ascii=False)

                return json_str

            _ = await cache_file(file_path, producer)
        except Exception as err:
            _logger.error('Kartgrunnlag download failed',
                          municipality_number=municipality_number, error=str(err))
            return []
        
    with file_path.open() as file:
        dataset_ids = json.load(file)

    return dataset_ids


async def _fetch_dataset_ids(municipality_number: str) -> List[str]:
    response = await _fetch_kartgrunnlag(municipality_number)
    contained_items: List[Dict[str, Any]] = response.get('containeditems', [])
    datasets: List[str] = []

    for dataset in contained_items:
        if dataset.get('ConfirmedDok') == 'JA' and dataset.get('dokStatus') == 'Godkjent':
            metadata_url: str = dataset.get('MetadataUrl')
            splitted = metadata_url.split('/')
            datasets.append(splitted[-1])

    return datasets


async def _fetch_kartgrunnlag(municipality_number: str) -> Dict[str, Any]:
    url = _API_BASE_URL + municipality_number

    async with get_semaphore():
        async with get_session().get(url) as response:
            response.raise_for_status()
            return await response.json()


__all__ = ['get_dataset_type', 'get_config_ids']
