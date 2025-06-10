import logging
from typing import Tuple, Dict
import aiohttp
import asyncio
from pydantic import HttpUrl
from osgeo import ogr
from ..utils.helpers.geometry import geometry_to_wkt
from ..utils.constants import WGS84_EPSG

_LOGGER = logging.getLogger(__name__)


async def query_ogc_api(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int = 4326, timeout: int = 30) -> Tuple[int, Dict]:
    wkt_str = geometry_to_wkt(geometry, epsg)
    # autopep8: off
    filter_crs = f'&filter-crs=http://www.opengis.net/def/crs/EPSG/0/{epsg}' if epsg != WGS84_EPSG else ''
    crs = f'&crs=http://www.opengis.net/def/crs/EPSG/0/{out_epsg}' if out_epsg != WGS84_EPSG else ''
    url = f'{base_url}/{layer}/items?f=json&filter-lang=cql2-text{filter_crs}{crs}&filter=S_INTERSECTS({geom_field},{wkt_str})'

    if filter:
        url += f' AND {filter}'
    # autopep8: on

    return await _query_ogc_api(url, timeout)


async def _query_ogc_api(url: str, timeout: int) -> Tuple[int, Dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                return 200, await response.json()
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


__all__ = ['query_ogc_api']
