from typing import Tuple, Dict
import asyncio
from pydantic import HttpUrl
from osgeo import ogr
from . import log_http_error, get_service_credentials, get_http_session
from ..models.config import DatasetConfig, FeatureService, Auth
from ..models.config.auth import Auth
from ..utils.helpers.geometry import geometry_to_arcgis_geom
from ..utils.constants import QUERY_TIMEOUT

_RESOURCE = 'ArcGIS REST API'


async def query_arcgis(
        arcgis: str | HttpUrl | FeatureService,
        layer: str,
        filter: str,
        geometry: ogr.Geometry,
        epsg: int,
        dataset_config: DatasetConfig | None = None
) -> Tuple[int, Dict]:
    url, auth = get_service_credentials(arcgis)
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

    return await _query_arcgis(api_url, auth, data, dataset_config)


async def _query_arcgis(url: str, auth: Auth, data: Dict, dataset_config: DatasetConfig | None) -> Tuple[int, Dict]:
    try:
        async with get_http_session(auth) as session:
            async with session.post(url, data=data, timeout=QUERY_TIMEOUT) as response:
                if response.status != 200:
                    log_http_error(
                        _RESOURCE, url, response.status, dataset=dataset_config)
                    return response.status, None

                json = await response.json()

                if 'error' in json:
                    log_http_error(_RESOURCE, url, 400,
                                       dataset=dataset_config)
                    return 400, None

                return 200, json
    except asyncio.TimeoutError:
        log_http_error(_RESOURCE, url, 408, dataset=dataset_config)
        return 408, None
    except Exception as err:
        log_http_error(_RESOURCE, url, 500,
                           dataset=dataset_config, err=err)
        return 500, None


__all__ = ['query_arcgis']
