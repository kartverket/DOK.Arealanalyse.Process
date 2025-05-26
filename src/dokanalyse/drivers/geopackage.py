import logging
import os
import json
from urllib.parse import urlparse
from typing import List, Dict, Tuple, Union
from pydantic import HttpUrl, FileUrl
from pathlib import Path
from osgeo import ogr, osr
import asyncio
import aiohttp
import aiofiles
from ..utils.helpers.geometry import transform_geometry, create_feature_collection
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import APP_FILES_DIR

_LOGGER = logging.getLogger(__name__)
_CACHE_DAYS = 86400


async def query_geopackage(url: Union[HttpUrl, FileUrl], filter: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> Dict:
    file_path = await _get_file_path(url, timeout)

    if not Path(file_path).exists():
        return None

    driver: ogr.Driver = ogr.GetDriverByName('GPKG')
    data_source: ogr.DataSource = driver.Open(file_path)
    layer: ogr.Layer = data_source.GetLayer(0)

    sr: osr.SpatialReference = layer.GetSpatialRef()
    auth_code: str = sr.GetAuthorityCode(None)
    gpkg_epsg = int(auth_code)

    if gpkg_epsg != epsg:
        input_geometry = transform_geometry(geometry, epsg, gpkg_epsg)
    else:
        input_geometry = geometry

    layer.SetSpatialFilter(input_geometry)

    if filter:
        layer.SetAttributeFilter(filter)

    feature: ogr.Feature
    features: List[Dict] = []

    for feature in layer:
        json_str = feature.ExportToJson()
        features.append(json.loads(json_str))

    response = create_feature_collection(features, gpkg_epsg)

    return response


async def _get_file_path(url: Union[HttpUrl, FileUrl], timeout: int) -> str:
    if url.scheme == 'file':
        return _file_uri_to_path(url)
    else:
        filename = _get_filename(url)
        file_path = Path(os.path.join(APP_FILES_DIR, f'geopackage/{filename}'))

        if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
            status, response = await _fetch_geopackage(url, timeout)

            if status != 200:
                return None

            file_path.parent.mkdir(parents=True, exist_ok=True)

            file = await aiofiles.open(file_path, mode='wb')
            await file.write(response)
            await file.close()

        return file_path.absolute()


async def _fetch_geopackage(url: HttpUrl, timeout) -> Tuple[int, bytes]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                return 200, await response.read()
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


def _get_filename(url: HttpUrl) -> str:
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)

    return filename.lower()


def _file_uri_to_path(file_uri: FileUrl) -> str:
    parsed = urlparse(str(file_uri))

    return os.path.abspath(os.path.join(parsed.netloc, parsed.path))
