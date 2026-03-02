from pathlib import Path
from typing import Any, Dict
import asyncio
import aiohttp
from .caching import get_or_create_file, should_refresh_cache
from ..constants import CACHE_DIR

_API_BASE_URL = 'https://kartkatalog.geonorge.no/api/getdata'
_CACHE_DAYS = 2

_kartkatalog_cache_dir = Path(CACHE_DIR).joinpath('kartkatalog')
_kartkatalog_cache_dir.mkdir(parents=True, exist_ok=True)


async def get_or_create_kartkatalog_metadata(
    metadata_id: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    url = f'{_API_BASE_URL}/{metadata_id}'
    path = _kartkatalog_cache_dir.joinpath(f'{metadata_id}.json')

    if path.exists() and not should_refresh_cache(path, days=_CACHE_DAYS):
        return path

    async def mapper(data: Dict[str, Any]) -> Dict[str, Any]:
        return _map_data(data, metadata_id)

    return await get_or_create_file(url, path, session, with_lock=with_lock, mapper=mapper, semaphore=semaphore)


def _map_data(data: Dict[str, Any], metadata_id: str) -> Dict[str, Any]:
    title = data.get('NorwegianTitle')
    description = data.get('Abstract')
    contact_owner: Dict[str, Any] = data.get('ContactOwner', {})
    owner = contact_owner.get('Organization')
    updated = data.get('DateUpdated')
    dataset_description_uri = f'https://kartkatalog.geonorge.no/metadata/{metadata_id}'

    return {
        'datasetId': metadata_id,
        'title': title,
        'description': description,
        'owner': owner,
        'updated': updated,
        'datasetDescriptionUri': dataset_description_uri
    }


__all__ = ['get_or_create_kartkatalog_metadata']
