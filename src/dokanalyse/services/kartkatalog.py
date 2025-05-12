from os import path
import json
from pathlib import Path
from uuid import UUID
from typing import Dict
import aiohttp
from ..models.metadata import Metadata
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import CACHE_DIR

_API_BASE_URL = 'https://kartkatalog.geonorge.no/api/getdata'
_CACHE_DAYS = 2

async def get_kartkatalog_metadata(metadata_id: UUID) -> Metadata:
    if metadata_id is None:
        return None

    metadata = await _get_kartkatalog_metadata(metadata_id)

    if metadata is None:
        return None

    return Metadata.from_dict(metadata)


async def _get_kartkatalog_metadata(metadata_id: UUID) -> Dict:
    file_path = Path(
        path.join(CACHE_DIR, f'kartkatalog/{metadata_id}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        response = await _fetch_kartkatalog_metadata(metadata_id)

        if response is None:
            return None

        metadata = _map_response(metadata_id, response)
        json_object = json.dumps(metadata, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return metadata
    else:
        with file_path.open(encoding='utf-8') as file:
            metadata = json.load(file)

        return metadata


def _map_response(metadata_id: UUID, response: Dict) -> Dict:
    title = response.get('NorwegianTitle')
    description = response.get('Abstract')
    owner = response.get('ContactOwner', {}).get('Organization')
    updated = response.get('DateUpdated')
    dataset_description_uri = 'https://kartkatalog.geonorge.no/metadata/' + str(metadata_id)

    return {
        'datasetId': str(metadata_id),
        'title': title,
        'description': description,
        'owner': owner,
        'updated': updated,
        'datasetDescriptionUri': dataset_description_uri
    }


async def _fetch_kartkatalog_metadata(metadata_id: UUID) -> Dict:
    try:
        url = f'{_API_BASE_URL}/{str(metadata_id)}'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


__all__ = ['get_kartkatalog_metadata']
