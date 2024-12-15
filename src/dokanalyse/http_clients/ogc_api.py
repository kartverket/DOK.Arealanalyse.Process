import aiohttp
import asyncio
from pydantic import HttpUrl
from osgeo import ogr
from ..utils.helpers.geometry import geometry_to_wkt
from ..utils.constants import WGS84_EPSG


async def query_ogc_api(base_url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, epsg: int, out_epsg: int = 4326, timeout: int = 30) -> tuple[int, dict]:
    wkt_str = geometry_to_wkt(geometry, epsg)
    filter_crs = f'&filter-crs=http://www.opengis.net/def/crs/EPSG/0/{epsg}' if epsg != WGS84_EPSG else ''
    crs = f'&crs=http://www.opengis.net/def/crs/EPSG/0/{out_epsg}' if out_epsg != WGS84_EPSG else ''
    url = f'{base_url}/{layer}/items?filter-lang=cql2-text{filter_crs}{crs}&filter=S_INTERSECTS({geom_field},{wkt_str})'

    return await _query_ogc_api(url, timeout)


async def _query_ogc_api(url: str, timeout: int) -> tuple[int, dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                return 200, await response.json()
    except asyncio.TimeoutError:
        return 408, None
    except:
        return 500, None


__all__ = ['query_ogc_api']