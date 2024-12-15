from os import path
import json
from pathlib import Path
from uuid import UUID
from typing import Dict
import aiohttp
from ..models.metadata import Metadata
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import CACHE_DIR

_CACHE_DAYS = 2


async def get_kartkatalog_metadata(dataset_id: UUID) -> Metadata:
    if dataset_id is None:
        return None

    metadata = await _get_kartkatalog_metadata(dataset_id)

    if metadata is None:
        return None

    return Metadata.from_dict(metadata)


async def _get_kartkatalog_metadata(dataset_id: UUID) -> Dict:
    file_path = Path(
        path.join(CACHE_DIR, f'kartkatalog/{dataset_id}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        response = await _fetch_kartkatalog_metadata(dataset_id)

        if response is None:
            return None

        metadata = _map_response(dataset_id, response)
        json_object = json.dumps(metadata, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return metadata
    else:
        with file_path.open(encoding='utf-8') as file:
            metadata = json.load(file)

        return metadata


def _map_response(dataset_id: UUID, response: Dict) -> Dict:
    title = response.get('NorwegianTitle')
    description = response.get('Abstract')
    owner = response.get('ContactOwner', {}).get('Organization')
    updated = response.get('DateUpdated')
    dataset_description_uri = f'https://kartkatalog.geonorge.no/metadata/{
        str(dataset_id)}'

    return {
        'datasetId': str(dataset_id),
        'title': title,
        'description': description,
        'owner': owner,
        'updated': updated,
        'datasetDescriptionUri': dataset_description_uri
    }


async def _fetch_kartkatalog_metadata(dataset_id: UUID) -> Dict:
    try:
        url = f'https://kartkatalog.geonorge.no/api/getdata/{str(dataset_id)}'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


__all__ = ['get_kartkatalog_metadata']
