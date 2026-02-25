from os import path
from uuid import UUID
from typing import Dict
import json
from async_lru import alru_cache
from ..utils.event_loop_manager import get_session, get_semaphore

_GEOLETT_API_URL = 'https://register.geonorge.no/geolett/api'
_CACHE_TTL = 86400 * 7

_local_geolett_ids = ['0c5dc043-e5b3-4349-8587-9b464d013aaa']


async def get_guidance_data(id: UUID) -> Dict:
    if id is None:
        return None

    if id in _local_geolett_ids:
        guidance_data = _fetch_local_guidance_data()
    else:
        guidance_data = await _fetch_guidance_data()

    result = list(filter(lambda item: item['id'] == str(id), guidance_data))

    return result[0] if len(result) > 0 else None


@alru_cache(maxsize=32, ttl=_CACHE_TTL)
async def _fetch_guidance_data() -> Dict:
    try:
        async with get_semaphore():
            async with get_session().get(_GEOLETT_API_URL) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


def _fetch_local_guidance_data() -> Dict:
    dir_path = path.dirname(path.realpath(__file__))

    file_path = path.join(
        path.dirname(dir_path), 'resources/geolett.local.json')

    with open(file_path, 'r') as file:
        return json.load(file)


__all__ = ['get_guidance_data']
