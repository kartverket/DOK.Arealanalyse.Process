from pathlib import Path
import asyncio
import aiohttp
from .caching import get_or_create_file, CacheUnit
from ..constants import CACHE_DIR

WMTS_CAPABILITIES_URL = 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml?request=GetCapabilities'
_FILENAME = 'wmts-capabilities.xml'
_CACHE_DAYS = 1


async def get_or_create_wmts_capabilities(
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    """The capabilities of Kartverket's WMTS, used for the basemap in map images."""
    path = Path(CACHE_DIR).joinpath(_FILENAME)

    return await get_or_create_file(
        WMTS_CAPABILITIES_URL,
        path,
        session,
        with_lock,
        semaphore=semaphore,
        timeout=30,
        cache=(_CACHE_DAYS, CacheUnit.DAYS)
    )


__all__ = ['get_or_create_wmts_capabilities', 'WMTS_CAPABILITIES_URL']
