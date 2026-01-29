from typing import Tuple, Dict
from pydantic import HttpUrl
from osgeo import ogr
from . import get_service_credentials, log_http_error
from .gdal import query_gdal
from ..models.config import DatasetConfig, FeatureService

_RESOURCE = 'OGC API'


def query_ogc_api(
        ogc_api: str | HttpUrl | FeatureService,
        layer: str,
        geometry: ogr.Geometry,
        filter: str,
        epsg: int,
        dataset_config: DatasetConfig | None = None
) -> Tuple[int, Dict | None]:
    url, auth = get_service_credentials(ogc_api)

    try:
        response = query_gdal('OAPIF', 'OAPIF:' + url, filter,
                              geometry, epsg, layer=layer, auth=auth)

        return 200, response
    except Exception as err:
        err_msg = str(err)

        if 'HTTP error code : 401' in err_msg:
            log_http_error(_RESOURCE, url, 401, dataset=dataset_config)
            return 401, None

        if 'timed out' in err_msg:
            log_http_error(_RESOURCE, url, 408, dataset=dataset_config)
            return 408, None

        log_http_error(_RESOURCE, url, 500, dataset=dataset_config, err=err)
        return 500, None


__all__ = ['query_ogc_api']
