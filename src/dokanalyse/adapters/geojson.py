import os
from urllib.parse import urlparse
from typing import Dict, Tuple, Union
from pydantic import HttpUrl, FileUrl
from osgeo import ogr
import aiohttp
import asyncio
from async_lru import alru_cache
from . import log_http_error
from .gdal import query_gdal
from ..utils.constants import QUERY_TIMEOUT

_CACHE_TTL = 86400
_RESOURCE = 'GeoJSON'

async def query_geojson(url: Union[HttpUrl, FileUrl], filter: str, geometry: ogr.Geometry, epsg: int) -> Dict | None:
    geojson = await _get_geojson(url)

    if not geojson:
        return None

    return query_gdal('GeoJSON', geojson, filter, geometry, epsg)


@alru_cache(maxsize=32, ttl=_CACHE_TTL)
async def _get_geojson(url: Union[HttpUrl, FileUrl]) -> str | None:
    if url.scheme == 'file':
        geojson = _load_geojson(url)
    else:
        _, geojson = await _fetch_geojson(str(url))

    return geojson


async def _fetch_geojson(url: str) -> Tuple[int, str | None]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=QUERY_TIMEOUT) as response:
                if response.status != 200:
                    log_http_error(_RESOURCE, url, response.status)
                    return response.status, None

                json_str = await response.text()

                return 200, json_str
    except asyncio.TimeoutError:
        log_http_error(_RESOURCE, url, 408)
        return 408, None
    except Exception as err:
        log_http_error(_RESOURCE, url, 500, err=err)
        return 500, None


def _load_geojson(file_uri: FileUrl) -> str | None:
    path = _file_uri_to_path(file_uri)

    try:
        with open(path) as file:
            return file.read()
    except:
        return None


def _file_uri_to_path(file_uri: FileUrl) -> str:
    parsed = urlparse(str(file_uri))

    return os.path.abspath(os.path.join(parsed.netloc, parsed.path))


__all__ = ['query_geojson']
