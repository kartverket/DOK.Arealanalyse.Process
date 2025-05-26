import logging
from typing import Tuple, Dict
import aiohttp
import asyncio
from pydantic import HttpUrl
from osgeo import ogr
from ..utils.helpers.geometry import geometry_to_arcgis_geom

_LOGGER = logging.getLogger(__name__)


async def query_arcgis(url: HttpUrl, layer: str, filter: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> Tuple[int, Dict]:
    api_url = f'{url}/{layer}/query'
    arcgis_geom = geometry_to_arcgis_geom(geometry, epsg)

    data = {
        'geometry': arcgis_geom,
        'geometryType': 'esriGeometryPolygon',
        'spatialRel': 'esriSpatialRelIntersects',
        'where': filter if filter is not None else '1=1',
        'inSR': epsg,
        'outSR': epsg,
        'units': 'esriSRUnit_Meter',
        'outFields': '*',
        'returnGeometry': True,
        'f': 'geojson'
    }

    return await _query_arcgis(api_url, data, timeout)


async def _query_arcgis(url: HttpUrl, data: Dict, timeout: int) -> Tuple[int, Dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                json = await response.json()

                if 'error' in json:
                    return 400, None

                return 200, json
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


__all__ = ['query_arcgis']
