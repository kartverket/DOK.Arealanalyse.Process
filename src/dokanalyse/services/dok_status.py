from os import path
import json
from uuid import UUID
from pathlib import Path
from typing import List, Dict, Tuple
import aiohttp
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import CACHE_DIR

_API_URL = 'https://register.geonorge.no/api/dok-statusregisteret.json'

_CACHE_DAYS = 7

_CATEGEORY_MAPPINGS = {
    'BuildingMatter': ('egnethet_byggesak', 'Byggesak'),
    'MunicipalLandUseElementPlan': ('egnethet_kommuneplan', 'Kommuneplan'),
    'ZoningPlan': ('egnethet_reguleringsplan', 'Reguleringsplan')
}

_VALUE_MAPPINGS = {
    0: 'Ikke egnet',
    1: 'Dårlig egnet',
    2: 'Noe egnet',
    3: 'Egnet',
    4: 'Godt egnet',
    5: 'Svært godt egnet'
}


async def get_dok_status_for_dataset(metadata_id: UUID) -> Dict:
    dok_status_all = await get_dok_status()

    for dok_status in dok_status_all:
        if dok_status.get('dataset_id') == str(metadata_id):
            return dok_status

    return None


async def get_dok_status() -> List[Dict]:
    file_path = Path(
        path.join(CACHE_DIR, 'dok-status.json'))

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        dok_status = await _get_dok_status()
        json_object = json.dumps(dok_status, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return dok_status
    else:
        with file_path.open(encoding='utf-8') as file:
            dok_status = json.load(file)

        return dok_status


async def _get_dok_status() -> List[Dict]:
    response = await _fetch_dok_status()

    if response is None:
        return []

    contained_items: List[Dict] = response.get('containeditems', [])
    datasets: List[Dict] = []

    for item in contained_items:
        dataset_id = _get_dataset_id(item)
        categories = _get_relevant_categories(item)
        suitability = []

        for key, value in categories:
            id, name = _CATEGEORY_MAPPINGS.get(key)

            suitability.append({
                'quality_dimension_id': id,
                'quality_dimension_name': name,
                'value': value,
                'comment': _VALUE_MAPPINGS.get(value)
            })

        datasets.append({
            'dataset_id': dataset_id,
            'suitability': suitability
        })

    return datasets


async def _fetch_dok_status() -> Dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(_API_URL) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


def _get_dataset_id(item) -> List[Tuple]:
    metadata_url: str = item.get('MetadataUrl')
    dataset_id = metadata_url.split('/')[-1]

    return dataset_id


def _get_relevant_categories(item) -> List[Tuple]:
    suitability: Dict = item.get('Suitability')
    categories = [(key, value) for key, value in suitability.items()
                  if key in _CATEGEORY_MAPPINGS.keys()]

    return categories


__all__ = ['get_dok_status_for_dataset', 'get_dok_status']
