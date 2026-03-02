from pathlib import Path
from typing import Any, Dict, List
import asyncio
import aiohttp
from .caching import get_or_create_file, should_refresh_cache
from ..constants import CACHE_DIR

_CACHE_DAYS = 14

_codelists_cache_dir = Path(CACHE_DIR).joinpath('codelists')
_codelists_cache_dir.mkdir(parents=True, exist_ok=True)


async def get_or_create_codelist(
    url: str,
    type: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    path = _codelists_cache_dir.joinpath(f'{type}.json')

    if path.exists() and not should_refresh_cache(path, days=_CACHE_DAYS):
        return path

    async def mapper(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _map_data(data)

    return await get_or_create_file(url, path, session, with_lock=with_lock, mapper=mapper, semaphore=semaphore)


def _map_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    contained_items: List[Dict[str, Any]] = data.get('containeditems', [])
    entries: List[Dict[str, Any]] = []

    for item in contained_items:
        if item.get('status') == 'Gyldig':
            entries.append({
                'value': item.get('codevalue'),
                'label': item.get('label'),
                'description': item.get('description')
            })

    return entries


__all__ = ['get_or_create_codelist']
