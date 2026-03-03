from os import path
from typing import Tuple
from pydantic import HttpUrl
import asyncio
from osgeo import ogr
from . import log_http_error, get_service_credentials, get_auth
from ..models.config import DatasetConfig, FeatureService, Auth
from ..utils.http_context import get_session

_RESOURCE = 'WFS'


async def query_wfs(
        wfs: str | HttpUrl | FeatureService,
        layer: str,
        geom_field: str,
        geometry: ogr.Geometry,
        epsg: int,
        dataset_config: DatasetConfig | None = None
) -> Tuple[int, bytes | None]:
    gml_str = geometry.ExportToGML(['FORMAT=GML3'])
    request_xml = _create_wfs_request_xml(layer, geom_field, gml_str, epsg)
    url, auth = get_service_credentials(wfs)

    return await _query_wfs(url, auth, request_xml, dataset_config)


def _create_wfs_request_xml(layer: str, geom_field: str, gml_str: str, epsg: int) -> bytes:
    dir_path = path.dirname(path.realpath(__file__))
    file_path = path.join(dir_path, 'wfs_request.xml.txt')

    with open(file_path, 'r') as file:
        file_text = file.read()

    return file_text.format(layer=layer,  geom_field=geom_field, geometry=gml_str, epsg=epsg).encode('utf-8')


async def _query_wfs(
    base_url: str,
    auth: Auth | None,
    xml_body: bytes,
    dataset_config: DatasetConfig | None
) -> Tuple[int, bytes | None]:
    url = f'{base_url}?service=WFS&version=2.0.0'
    auth_params = get_auth(auth)

    try:
        async with get_session().post(url, data=xml_body, **auth_params) as response:
            if response.status == 200:
                return response.status, await response.read()

            log_http_error(_RESOURCE, url, response.status,
                           dataset=dataset_config)

            return response.status, None
    except asyncio.TimeoutError:
        log_http_error(_RESOURCE, url, 408, dataset=dataset_config)
        return 408, None
    except Exception as err:
        log_http_error(_RESOURCE, url, 500, dataset=dataset_config, err=err)
        return 500, None


__all__ = ['query_wfs']
