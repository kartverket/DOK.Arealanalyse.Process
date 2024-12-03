from os import path
import json
from uuid import UUID
from pathlib import Path
from typing import List
import aiohttp
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import APP_FILES_DIR

__CACHE_DAYS = 7
__API_URL = 'https://register.geonorge.no/api/dok-statusregisteret.json'

__CATEGEORY_MAPPINGS = {
    'BuildingMatter': ('egnethet_byggesak', 'Byggesak'),
    'MunicipalLandUseElementPlan': ('egnethet_kommuneplan', 'Kommuneplan'),
    'ZoningPlan': ('egnethet_reguleringsplan', 'Reguleringsplan')
}

__VALUE_MAPPINGS = {
    0: 'Ikke egnet',
    1: 'Dårlig egnet',
    2: 'Noe egnet',
    3: 'Egnet',
    4: 'Godt egnet',
    5: 'Svært godt egnet'
}


async def get_dok_status_for_dataset(dataset_id: UUID) -> dict:
    dok_status_all = await get_dok_status()

    for dok_status in dok_status_all:
        if dok_status.get('dataset_id') == str(dataset_id):
            return dok_status

    return None


async def get_dok_status() -> List[dict]:
    file_path = Path(
        path.join(APP_FILES_DIR, 'resources/dok-status.json'))

    if not file_path.exists() or should_refresh_cache(file_path, __CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        dok_status = await __get_dok_status()
        json_object = json.dumps(dok_status, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return dok_status
    else:
        with file_path.open(encoding='utf-8') as file:
            dok_status = json.load(file)

        return dok_status


async def __get_dok_status() -> List[dict]:
    response = await __fetch_dok_status()

    if response is None:
        return []

    contained_items = response.get('containeditems', [])
    datasets = []

    for item in contained_items:
        dataset_id = __get_dataset_id(item)
        categories = __get_relevant_categories(item)
        suitability = []

        for key, value in categories:
            id, name = __CATEGEORY_MAPPINGS.get(key)

            suitability.append({
                'quality_dimension_id': id,
                'quality_dimension_name': name,
                'value': value,
                'comment': __VALUE_MAPPINGS.get(value)
            })

        datasets.append({
            'dataset_id': dataset_id,
            'suitability': suitability
        })

    return datasets


async def __fetch_dok_status() -> dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(__API_URL) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


def __get_dataset_id(item) -> List[tuple]:
    metadata_url: str = item.get('MetadataUrl')
    dataset_id = metadata_url.split('/')[-1]

    return dataset_id


def __get_relevant_categories(item) -> List[tuple]:
    suitability: dict = item.get('Suitability')
    categories = [(key, value) for key, value in suitability.items()
                  if key in __CATEGEORY_MAPPINGS.keys()]

    return categories
