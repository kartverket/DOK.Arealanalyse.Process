from os import path
from uuid import UUID
import json
import aiohttp
from async_lru import alru_cache

__LOCAL_GEOLETT_IDS = [
    '0c5dc043-e5b3-4349-8587-9b464d013aaa'
]


async def get_geolett_data(id: UUID) -> dict:
    if id is None:
        return None

    if id in __LOCAL_GEOLETT_IDS:
        geolett = __fetch_local_geolett_data()
    else:
        geolett = await __fetch_geolett_data()

    result = list(filter(lambda item: item['id'] == str(id), geolett))

    return result[0] if len(result) > 0 else None


@alru_cache(maxsize=32, ttl=86400*7)
async def __fetch_geolett_data() -> dict:
    try:
        url = 'https://register.geonorge.no/geolett/api/'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


def __fetch_local_geolett_data() -> dict:
    dir_path = path.dirname(path.realpath(__file__))

    file_path = path.join(
        path.dirname(dir_path), 'resources/geolett.local.json')

    with open(file_path, 'r') as file:
        return json.load(file)
