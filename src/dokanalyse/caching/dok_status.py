from pathlib import Path
from typing import Any, Dict, List, Tuple
import asyncio
import aiohttp
from .caching import get_or_create_file, should_refresh_cache
from ..constants import CACHE_DIR

_API_URL = 'https://register.geonorge.no/api/dok-statusregisteret.json'
_FILENAME = 'dok-status.json'
_CACHE_DAYS = 1

_dokstatus_category_mappings = {
    'BuildingMatter': ('egnethet_byggesak', 'Byggesak'),
    'MunicipalLandUseElementPlan': ('egnethet_kommuneplan', 'Kommuneplan'),
    'ZoningPlan': ('egnethet_reguleringsplan', 'Reguleringsplan')
}

_dokstatus_value_mappings = {
    0: 'Ikke egnet',
    1: 'Dårlig egnet',
    2: 'Noe egnet',
    3: 'Egnet',
    4: 'Godt egnet',
    5: 'Svært godt egnet'
}


async def get_or_create_dok_status(
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    path = Path(CACHE_DIR).joinpath(_FILENAME)

    if path.exists() and not should_refresh_cache(path, days=_CACHE_DAYS):
        return path

    async def mapper(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _map_data(data)

    return await get_or_create_file(_API_URL, path, session, with_lock=with_lock, mapper=mapper, semaphore=semaphore)


def _map_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    contained_items: List[Dict[str, Any]] = data.get('containeditems', [])
    datasets: List[Dict[str, Any]] = []

    for item in contained_items:
        dataset_id = _get_dataset_id(item)
        theme: str = item.get('theme', '')
        categories = _get_relevant_categories(item)
        suitability = []

        for key, value in categories:
            if key not in _dokstatus_category_mappings:
                continue

            id, name = _dokstatus_category_mappings[key]

            suitability.append({
                'quality_dimension_id': id,
                'quality_dimension_name': name,
                'value': value,
                'comment': _dokstatus_value_mappings.get(value)
            })

        datasets.append({
            'dataset_id': dataset_id,
            'theme': theme,
            'suitability': suitability
        })

    return datasets


def _get_dataset_id(item: Dict[str, Any]) -> str:
    metadata_url: str = item['MetadataUrl']
    dataset_id = metadata_url.split('/')[-1]

    return dataset_id


def _get_relevant_categories(item: Dict[str, Any]) -> List[Tuple[str, int]]:
    suitability: Dict[str, int] = item['Suitability']
    categories = [(key, value) for key, value in suitability.items()
                  if key in _dokstatus_category_mappings.keys()]

    return categories


__all__ = ['get_or_create_dok_status']
