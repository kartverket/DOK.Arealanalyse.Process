from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from osgeo import gdal, ogr, osr
from . import get_credential
from ..models.config.auth import Basic, ApiKey
from ..utils.helpers.geometry import create_feature_collection, transform_geometry
from ..utils.helpers.gdal import normalize_object
from ..constants import CLIENT_TIMEOUT

gdal.SetConfigOption('GDAL_HTTP_CONNECTTIMEOUT', str(CLIENT_TIMEOUT))
gdal.SetConfigOption('GDAL_HTTP_TIMEOUT', str(CLIENT_TIMEOUT))

_AUTH_OPTIONS = ['GDAL_HTTP_HEADERS', 'GDAL_HTTP_AUTH', 'GDAL_HTTP_USERPWD']

_logger: BoundLogger = structlog.get_logger(__name__)


def query_gdal(
    driver_name: str,
    data_source: Any,
    filter: str | None,
    geometry: ogr.Geometry,
    epsg: int,
    **kwargs
) -> Dict[str, Any]:
    """Queries a GDAL data source. Blocking, run it with asyncio.to_thread.

    Credentials are set as thread-local config options, so concurrent queries
    in other threads never see them.
    """
    auth: Basic | ApiKey | None = kwargs.get('auth')
    layer_name: str | None = kwargs.get('layer')

    try:
        if auth:
            _set_auth_options(auth)

        return _query(driver_name, data_source, layer_name, filter, geometry, epsg)
    except Exception as err:
        _logger.error('GDAL error', driver=driver_name, error=str(err))
        raise
    finally:
        if auth:
            _unset_auth_options()


def _query(
    driver_name: str,
    data_source: Any,
    layer_name: str | None,
    filter: str | None,
    geometry: ogr.Geometry,
    epsg: int
) -> Dict[str, Any]:
    driver: gdal.Driver = ogr.GetDriverByName(driver_name)
    ds: gdal.Dataset | None = driver.Open(data_source)

    if ds is None:
        raise ValueError(f'Could not open data source with driver {driver_name}')

    layer: ogr.Layer | None = ds.GetLayerByName(
        layer_name) if layer_name else ds.GetLayer(0)

    if layer is None:
        raise ValueError(f'Layer "{layer_name}" not found in data source')

    target_epsg = _get_layer_epsg(layer) or epsg

    if target_epsg != epsg:
        input_geometry = transform_geometry(geometry, epsg, target_epsg)
    else:
        input_geometry = geometry

    layer.SetSpatialFilter(input_geometry)

    if filter:
        layer.SetAttributeFilter(filter)

    ogr_feature: ogr.Feature
    features: List[Dict[str, Any]] = []

    for ogr_feature in layer:
        feature_geom: ogr.Geometry | None = ogr_feature.GetGeometryRef()

        if feature_geom is None:
            continue

        # Features are returned in the CRS of the query geometry
        if target_epsg != epsg:
            clone = transform_geometry(feature_geom, target_epsg, epsg)
        else:
            clone = feature_geom.Clone()

        ogr_feature.SetGeometryDirectly(None)
        json_dict: Any = ogr_feature.ExportToJson(as_object=True)
        properties: Dict[str, Any] = normalize_object(json_dict['properties'])

        features.append({
            'geometry': clone,
            'properties': properties
        })

    return create_feature_collection(features, epsg)


def _get_layer_epsg(layer: ogr.Layer) -> int | None:
    sr: osr.SpatialReference | None = layer.GetSpatialRef()

    if sr is None:
        return None

    auth_code: str | None = sr.GetAuthorityCode(None)

    return int(auth_code) if auth_code else None


def _set_auth_options(auth: ApiKey | Basic) -> None:
    if isinstance(auth, ApiKey):
        gdal.SetThreadLocalConfigOption(
            'GDAL_HTTP_HEADERS', f'X-API-KEY: {get_credential(auth.api_key)}')
    else:
        gdal.SetThreadLocalConfigOption('GDAL_HTTP_AUTH', 'BASIC')
        gdal.SetThreadLocalConfigOption(
            'GDAL_HTTP_USERPWD', f'{get_credential(auth.username)}:{get_credential(auth.password)}')


def _unset_auth_options() -> None:
    for option in _AUTH_OPTIONS:
        gdal.SetThreadLocalConfigOption(option, None)


__all__ = ['query_gdal']
