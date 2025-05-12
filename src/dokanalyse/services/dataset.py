from os import path
from pathlib import Path
import json
from uuid import UUID
from typing import List, Dict, Literal
import aiohttp
from ..models.config import DatasetConfig
from ..services.config import get_dataset_configs
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import CACHE_DIR

_API_BASE_URL = 'https://register.geonorge.no/api/det-offentlige-kartgrunnlaget-kommunalt.json?municipality='
_CACHE_DAYS = 7


def get_dataset_type(config: DatasetConfig) -> Literal['wfs', 'arcgis', 'ogc_api']:
    if config.wfs is not None:
        return 'wfs'
    elif config.arcgis is not None:
        return 'arcgis'
    elif config.ogc_api is not None:
        return 'ogc_api'

    return None


async def get_config_ids(data: dict, municipality_number: str) -> Dict[UUID, bool]:
    include_chosen_dok = data.get('includeFilterChosenDOK', True)

    if include_chosen_dok:
        kartgrunnlag = await _get_kartgrunnlag(municipality_number)
    else:
        kartgrunnlag = []

    config_ids = _get_datasets_by_theme(data.get('theme'))
    datasets: Dict[UUID, bool] = {}

    for id in config_ids:
        should_analyze = len(
            kartgrunnlag) == 0 or id is None or id in kartgrunnlag
        datasets[id] = should_analyze

    return datasets


def _get_datasets_by_theme(theme: str) -> List[UUID]:
    dataset_configs = get_dataset_configs()
    config_ids = []

    for config in dataset_configs:
        themes = list(map(lambda theme: theme.lower(), config.themes))

        if theme is None or theme.lower() in themes:
            config_ids.append(config.config_id)

    return config_ids


async def _get_kartgrunnlag(municipality_number: str) -> List[str]:
    if municipality_number is None:
        return []

    file_path = Path(path.join(
        CACHE_DIR, 'dok-datasets', f'{municipality_number}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_ids = await _fetch_dataset_ids(municipality_number)
        json_object = json.dumps(dataset_ids, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return dataset_ids
    else:
        with file_path.open(encoding='utf-8') as file:
            dataset_ids = json.load(file)

        return dataset_ids


async def _fetch_dataset_ids(municipality_number: str) -> List[str]:
    response = await _fetch_kartgrunnlag(municipality_number)

    if response is None:
        return []

    contained_items: List[Dict] = response.get('containeditems', [])
    datasets: List[str] = []

    for dataset in contained_items:
        if dataset.get('ConfirmedDok') == 'JA' and dataset.get('dokStatus') == 'Godkjent':
            metadata_url: str = dataset.get('MetadataUrl')
            splitted = metadata_url.split('/')
            datasets.append(splitted[-1])

    return datasets


async def _fetch_kartgrunnlag(municipality_number: str) -> dict:
    try:
        url = _API_BASE_URL + municipality_number

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


__all__ = ['get_dataset_type', 'get_config_ids']
