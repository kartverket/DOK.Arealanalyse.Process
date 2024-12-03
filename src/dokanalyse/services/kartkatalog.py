from os import path
import json
from pathlib import Path
from uuid import UUID
import aiohttp
from ..models.metadata import Metadata
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import APP_FILES_DIR

__CACHE_DAYS = 2


async def get_kartkatalog_metadata(dataset_id: UUID) -> Metadata:
    if dataset_id is None:
        return None
    
    metadata = await __get_kartkatalog_metadata(dataset_id)

    if metadata is None:
        return None

    return Metadata.from_dict(metadata)



async def __get_kartkatalog_metadata(dataset_id: UUID) -> dict:
    file_path = Path(
        path.join(APP_FILES_DIR, f'resources/kartkatalog/{dataset_id}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, __CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        response = await __fetch_kartkatalog_metadata(dataset_id)

        if response is None:
            return None

        metadata = __map_response(dataset_id, response)
        json_object = json.dumps(metadata, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return metadata
    else:
        with file_path.open(encoding='utf-8') as file:
            metadata = json.load(file)

        return metadata


def __map_response(dataset_id: UUID, response: dict) -> dict:
    title = response.get('NorwegianTitle')
    description = response.get('Abstract')
    owner = response.get('ContactOwner', {}).get('Organization')
    updated = response.get('DateUpdated')
    dataset_description_uri = 'https://kartkatalog.geonorge.no/metadata/' + str(dataset_id)

    return {
        'datasetId': str(dataset_id),
        'title': title,
        'description': description,
        'owner': owner,
        'updated': updated,
        'datasetDescriptionUri': dataset_description_uri
    }


async def __fetch_kartkatalog_metadata(dataset_id: UUID) -> dict:
    try:
        url = 'https://kartkatalog.geonorge.no/api/getdata/' + str(dataset_id)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None
