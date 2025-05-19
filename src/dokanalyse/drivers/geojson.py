import logging
import json
from typing import List, Dict, Tuple
from pydantic import AnyUrl, FileUrl
from osgeo import ogr, osr
import aiohttp
import asyncio
from ..utils.helpers.geometry import create_feature_collection

_LOGGER = logging.getLogger(__name__)


async def query_geojson(url: AnyUrl, filter: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> Dict:
    if url.scheme == 'file':
        geojson = _load_geojson(url)
    else:
        _, geojson = await _fetch_geojson(url, timeout)


    if not geojson:
        return None

    driver: ogr.Driver = ogr.GetDriverByName('GeoJSON')
    data_source: ogr.DataSource = driver.Open(geojson)
    layer: ogr.Layer = data_source.GetLayer(0)

    layer.SetSpatialFilter(geometry)

    if filter:
        layer.SetAttributeFilter(filter)

    feature: ogr.Feature
    features: List[Dict] = []

    for feature in layer:
        json_str = feature.ExportToJson()
        features.append(json.load(json_str))

    response = create_feature_collection(features)

    return response


async def _fetch_geojson(url: AnyUrl, timeout) -> Tuple[int, str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                json_str = await response.text()

                return 200, json_str
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


def _load_geojson(file_url: FileUrl) -> str:
    try:
        with open(str(file_url)) as file:
            return file.read()
    except:
        return None
