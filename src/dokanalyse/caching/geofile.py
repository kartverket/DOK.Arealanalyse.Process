from urllib.parse import urlparse
from pathlib import Path
from typing import Literal
import asyncio
import aiohttp
from .caching import get_or_create_file, should_refresh_cache
from ..constants import CACHE_DIR

_CACHE_DAYS = 365


async def get_or_create_geofile(
    url: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    filename = _get_filename(url)
    filetype = _get_filetype(url)

    dirpath = Path(CACHE_DIR).joinpath(
        filetype) if filetype else Path(CACHE_DIR)
    
    dirpath.mkdir(parents=True, exist_ok=True)

    path = dirpath.joinpath(filename)

    if path.exists() and not should_refresh_cache(path, days=_CACHE_DAYS):
        return path

    return await get_or_create_file(url, path, session, with_lock=with_lock, semaphore=semaphore)


def _get_filetype(url: str) -> Literal['geojson', 'geopackage'] | None:
    filename = _get_filename(url)

    if filename.endswith('.geojson') or filename.endswith('.json'):
        return 'geojson'

    if filename.endswith('.gpkg'):
        return 'geopackage'

    return None


def _get_filename(url: str) -> str:
    parsed = urlparse(str(url))
    filename = Path(parsed.path).name

    return filename.lower()


__all__ = ['get_or_create_geofile']