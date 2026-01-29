import os
from urllib.parse import urlparse
from typing import Dict, Tuple, Union
from pydantic import HttpUrl, FileUrl
from pathlib import Path
from osgeo import ogr
import asyncio
import aiohttp
import aiofiles
from . import log_http_error
from .gdal import query_gdal
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import APP_FILES_DIR, QUERY_TIMEOUT

_CACHE_DAYS = 86400
_RESOURCE = 'GeoPackage'


async def query_geopackage(url: Union[HttpUrl, FileUrl], filter: str, geometry: ogr.Geometry, epsg: int) -> Dict | None:
    file_path = await _get_file_path(url)

    if not file_path or not Path(file_path).exists():
        return None

    return query_gdal('GPKG', file_path, filter, geometry, epsg)


async def _get_file_path(url: Union[HttpUrl, FileUrl]) -> str | None:
    if url.scheme == 'file':
        return _file_uri_to_path(url)
    else:
        filename = _get_filename(url)
        file_path = Path(os.path.join(APP_FILES_DIR, f'geopackage/{filename}'))

        if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
            status, response = await _fetch_geopackage(url)

            if status != 200:
                return None

            file_path.parent.mkdir(parents=True, exist_ok=True)

            file = await aiofiles.open(file_path, mode='wb')
            await file.write(response)
            await file.close()

        return file_path.absolute()


async def _fetch_geopackage(url: HttpUrl) -> Tuple[int, bytes | None]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(str(url), timeout=QUERY_TIMEOUT) as response:
                if response.status != 200:
                    log_http_error(_RESOURCE, url, response.status)
                    return response.status, None
                    
                return 200, await response.read()
    except asyncio.TimeoutError:
        log_http_error(_RESOURCE, url, 408)
        return 408, None
    except Exception as err:
        log_http_error(_RESOURCE, url, 500, err=err)
        return 500, None


def _get_filename(url: HttpUrl) -> str:
    parsed = urlparse(str(url))
    filename = os.path.basename(parsed.path)

    return filename.lower()


def _file_uri_to_path(file_uri: FileUrl) -> str:
    parsed = urlparse(str(file_uri))

    return os.path.abspath(os.path.join(parsed.netloc, parsed.path))


__all__ = ['query_geopackage']