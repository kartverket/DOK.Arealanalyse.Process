from os import path
import logging
from typing import Tuple
from io import BytesIO
from lxml import etree as ET
from pydantic import HttpUrl
import aiohttp
import asyncio
from osgeo import ogr

_LOGGER = logging.getLogger(__name__)


async def query_wfs(url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> Tuple[int, str]:
    gml_str = geometry.ExportToGML(['FORMAT=GML3'])
    request_xml = _create_wfs_request_xml(layer, geom_field, gml_str, epsg)

    return await _query_wfs(url, request_xml, timeout)


def _create_wfs_request_xml(layer: str, geom_field: str, gml_str: str, epsg: int) -> str:
    dir_path = path.dirname(path.realpath(__file__))
    file_path = path.join(dir_path, 'wfs_request.xml.txt')

    with open(file_path, 'r') as file:
        file_text = file.read()

    return file_text.format(layer=layer,  geom_field=geom_field, geometry=gml_str, epsg=epsg).encode('utf-8')


async def _query_wfs(url: HttpUrl, xml_body: str, timeout: int) -> Tuple[int, str]:
    url = f'{url}?service=WFS&version=2.0.0'
    headers = {'Content-Type': 'application/xml'}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=xml_body, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return 200, await response.text()

                if response.status == 400:
                    _log_error_response(await response.text())

                return response.status, None
    except asyncio.TimeoutError:
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


def _log_error_response(response: str) -> None:
    source = BytesIO(response.encode('utf-8'))
    context = ET.iterparse(source)

    for _, elem in context:
        localname = ET.QName(elem).localname

        if localname != 'ExceptionText':
            continue

        _LOGGER.error(elem.text)


__all__ = ['query_wfs']
