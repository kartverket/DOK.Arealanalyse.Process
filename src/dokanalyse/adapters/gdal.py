import threading
from typing import Dict, List, Any
import structlog
from structlog.stdlib import BoundLogger
from osgeo import gdal, ogr, osr
from . import get_credential
from ..models.config.auth import Basic, ApiKey
from ..utils.helpers.geometry import create_feature_collection, transform_geometry
from ..utils.helpers.gdal import normalize_object
from ..utils.constants import QUERY_TIMEOUT
from ..utils.helpers.common import dbg

gdal_lock = threading.Lock()

gdal.SetConfigOption('GDAL_HTTP_CONNECTTIMEOUT', str(QUERY_TIMEOUT))
gdal.SetConfigOption('GDAL_HTTP_TIMEOUT', str(QUERY_TIMEOUT))

_LOGGER: BoundLogger = structlog.get_logger(__name__)


def query_gdal(driver_name: str, data_source: Any, filter: str, geometry: ogr.Geometry, epsg: int, **kwargs) -> Dict[str, Any] | None:
    auth: Basic | ApiKey | None = kwargs.get('auth')
    layer_name: str | None = kwargs.get('layer')

    try:
        if not auth:
            return _query(driver_name, data_source, layer_name, filter, geometry, epsg)

        with gdal_lock:
            _set_auth_options(auth)
            response = _query(driver_name, data_source,
                              layer_name, filter, geometry, epsg)
            _unset_auth_options()

        return response
    except Exception as err:
        _LOGGER.error('GDAL error', error=str(err))
        raise


def _query(driver_name: str, data_source: Any, layer_name: str | None, filter: str, geometry: ogr.Geometry, epsg: int) -> Dict[str, Any]:
    driver: ogr.Driver = ogr.GetDriverByName(driver_name)
    ds: ogr.DataSource = driver.Open(data_source)
    layer: ogr.Layer = ds.GetLayerByName(
        layer_name) if layer_name else ds.GetLayer(0)

    sr: osr.SpatialReference = layer.GetSpatialRef()
    auth_code: str = sr.GetAuthorityCode(None)
    target_epsg = int(auth_code)

    if target_epsg != epsg:
        input_geometry = transform_geometry(geometry, epsg, target_epsg)
    else:
        input_geometry = geometry

    layer.SetSpatialFilter(input_geometry)

    if filter:
        layer.SetAttributeFilter(filter)

    ogr_feature: ogr.Feature
    features: List[Dict] = []

    for ogr_feature in layer:
        feature_geom: ogr.Geometry = ogr_feature.GetGeometryRef()
        clone: ogr.Geometry = feature_geom.Clone()

        ogr_feature.SetGeometryDirectly(None)
        json_dict = ogr_feature.ExportToJson(as_object=True)
        properties = normalize_object(json_dict['properties'])

        feature = {
            'geometry': clone,
            'properties': properties
        }

        features.append(feature)

    response = create_feature_collection(features, target_epsg)

    return response


def _set_auth_options(auth: ApiKey | Basic) -> None:
    if isinstance(auth, ApiKey):
        gdal.SetConfigOption('GDAL_HTTP_HEADER', f'X-API-KEY: {auth.api_key}')
    else:
        gdal.SetConfigOption('GDAL_HTTP_AUTH', 'BASIC')
        gdal.SetConfigOption(
            'GDAL_HTTP_USERPWD', f'{get_credential(auth.username)}:{get_credential(auth.password)}')


def _unset_auth_options() -> None:
    gdal.SetConfigOption('GDAL_HTTP_HEADER', None)
    gdal.SetConfigOption('GDAL_HTTP_AUTH', None)
    gdal.SetConfigOption('GDAL_HTTP_USERPWD', None)


