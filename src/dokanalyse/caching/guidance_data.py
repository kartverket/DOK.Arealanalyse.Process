from pathlib import Path
from typing import Any, Dict, List
import asyncio
import aiohttp
from .caching import get_or_create_file, CacheUnit
from ..constants import CACHE_DIR

_API_URL = 'https://register.geonorge.no/geolett/api'
_FILENAME = 'guidance-data.json'
_CACHE_DAYS = 2


async def get_or_create_guidance_data(
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    path = Path(CACHE_DIR).joinpath(_FILENAME)

    async def mapper(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return _map_data(data)

    return await get_or_create_file(
        _API_URL, 
        path, 
        session, 
        with_lock,
        mapper=mapper, 
        semaphore=semaphore,
        cache=(_CACHE_DAYS, CacheUnit.DAYS)
    )


def _map_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []

    for item in data:
        mapped = {
            'id': item['id'],
            'title': item['tittel'],
            'description': item['forklarendeTekst'],
            'guidance_text': item['dialogtekst']
        }

        guidance_uri = []

        for link in item.get('lenker', []):
            guidance_uri.append({
                'href': link['href'],
                'title': link['tittel']
            })

        mapped['guidance_uri'] = guidance_uri
        possible_actions_str: str = item.get('muligeTiltak', '')
        possible_actions = []

        for line in possible_actions_str.splitlines():
            action = line.lstrip('- ').strip()

            if action != '':
                possible_actions.append(action)

        mapped['possible_actions'] = possible_actions

        output.append(mapped)

    return output


__all__ = ['get_or_create_guidance_data']
