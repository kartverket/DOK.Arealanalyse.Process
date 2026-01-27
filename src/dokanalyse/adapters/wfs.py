from os import path
import logging
from typing import Tuple
from pydantic import HttpUrl
import asyncio
from osgeo import ogr
from . import log_error_response, get_service_credentials, get_http_session
from ..models.config.feature_service import FeatureService
from ..models.config.auth import Auth

_LOGGER = logging.getLogger(__name__)


async def query_wfs(wfs: HttpUrl | FeatureService, layer: str, geom_field: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> Tuple[int, str]:
    gml_str = geometry.ExportToGML(['FORMAT=GML3'])
    request_xml = _create_wfs_request_xml(layer, geom_field, gml_str, epsg)
    url, auth = get_service_credentials(wfs)

    return await _query_wfs(url, auth, request_xml, timeout)


def _create_wfs_request_xml(layer: str, geom_field: str, gml_str: str, epsg: int) -> str:
    dir_path = path.dirname(path.realpath(__file__))
    file_path = path.join(dir_path, 'wfs_request.xml.txt')

    with open(file_path, 'r') as file:
        file_text = file.read()

    return file_text.format(layer=layer,  geom_field=geom_field, geometry=gml_str, epsg=epsg).encode('utf-8')


async def _query_wfs(base_url: HttpUrl, auth: Auth, xml_body: str, timeout: int) -> Tuple[int, str]:
    url = f'{base_url}?service=WFS&version=2.0.0'
    headers = {'Content-Type': 'application/xml'}

    try:
        async with get_http_session(auth) as session:
            async with session.post(url, data=xml_body, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return 200, await response.text()

                log_error_response(url, response.status)

                return response.status, None
    except asyncio.TimeoutError:
        _LOGGER.error(f'Request against WFS "{base_url}" timed out')
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


__all__ = ['query_wfs']
