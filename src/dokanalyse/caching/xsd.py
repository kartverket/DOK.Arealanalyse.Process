from pathlib import Path
import hashlib
import asyncio
import aiohttp
from .caching import get_or_create_file, should_refresh_cache
from ..constants import CACHE_DIR

_CACHE_DAYS = 30

_xsd_cache_dir = Path(CACHE_DIR).joinpath('xsds')
_xsd_cache_dir.mkdir(parents=True, exist_ok=True)


async def get_or_create_xml_schema(
    url: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    xsd_url = f'{url}?service=WFS&version=2.0.0&request=DescribeFeatureType'
    filename = f'{_hash_url(url)}.xsd'
    path = _xsd_cache_dir.joinpath(filename)

    if path.exists() and not should_refresh_cache(path, days=_CACHE_DAYS):
        return path

    return await get_or_create_file(xsd_url, path, session, with_lock=with_lock, semaphore=semaphore)


def _hash_url(url: str) -> str:
    url_bytes = url.encode('utf-8')
    hash_object = hashlib.sha256(url_bytes)
    hex_digest = hash_object.hexdigest()

    return hex_digest


__all__ = ['get_or_create_xml_schema']