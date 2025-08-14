import logging
import json
from typing import Tuple, Dict, Any
import aiohttp
import asyncio
from pydantic import HttpUrl
from cql2 import Expr
from osgeo import ogr
from . import log_error_response
from ..utils.helpers.geometry import geometry_to_wkt, transform_geometry
from ..utils.constants import WGS84_EPSG

_LOGGER = logging.getLogger(__name__)


async def query_ogc_api(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int = 4326, timeout: int = 30) -> Tuple[int, Dict]:
    has_search = await _has_search_endpoint(base_url)

    if has_search:
        return await _query_ogc_api_post(base_url, layer, geom_field, geometry, filter, epsg, out_epsg, timeout)

    return await _query_ogc_api_get(base_url, layer, geom_field, geometry, filter, epsg, out_epsg, timeout)


async def _query_ogc_api_get(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int = 4326, timeout: int = 30) -> Tuple[int, Dict]:
    url = _create_request_url(
        base_url, layer, geom_field, geometry, filter, epsg, out_epsg)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    log_error_response(url, response.status)
                    return response.status, None

                return 200, await response.json()
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


async def _query_ogc_api_post(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int = 4326, timeout: int = 30) -> Tuple[int, Dict]:
    request_body = _create_request_body(
        layer, geom_field, geometry, filter, epsg, out_epsg)

    json_str = json.dumps(request_body)

    url = f'{base_url}/search?f=json'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_str, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                return 200, await response.json()
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


def _create_request_url(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int = 4326) -> str:
    wkt_str = geometry_to_wkt(geometry, epsg)

    # autopep8: off
    filter_crs = f'&filter-crs=http://www.opengis.net/def/crs/EPSG/0/{epsg}' if epsg != WGS84_EPSG else ''
    crs = f'&crs=http://www.opengis.net/def/crs/EPSG/0/{out_epsg}' if out_epsg != WGS84_EPSG else ''
    url = f'{base_url}/collections/{layer}/items?f=json&filter-lang=cql2-text{filter_crs}{crs}&filter=S_INTERSECTS({geom_field},{wkt_str})'

    if filter:
        url += f' AND {filter}'
    # autopep8: on

    return url


def _create_request_body(layer: str, geom_field: str, geometry: ogr.Geometry, filter: str, epsg: int, out_epsg: int) -> Dict[str, Any]:
    transd = transform_geometry(geometry, epsg, WGS84_EPSG)
    transd.SwapXY()

    wkt_str = geometry_to_wkt(transd, WGS84_EPSG)
    cql2_text = f'S_INTERSECTS({geom_field}, {wkt_str})'

    if filter:
        cql2_text += f' AND {filter}'

    expr = Expr(cql2_text)

    cql2_json = {
        'collections': [layer],
        'filter': expr.to_json()
    }

    if out_epsg != WGS84_EPSG:
        cql2_json['crs'] = f'http://www.opengis.net/def/crs/EPSG/0/{out_epsg}'

    cql2_json['collections'] = [layer]
    cql2_json['filter']

    return cql2_json


async def _has_search_endpoint(base_url: str) -> bool:
    url = f'{base_url}/search'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as response:
                return response.status < 400
    except Exception as err:
        _LOGGER.error(err)
        return False


__all__ = ['query_ogc_api']
