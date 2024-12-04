from os import path
from pydantic import HttpUrl
import aiohttp
import asyncio
from osgeo import ogr

__DIR_PATH = path.dirname(path.realpath(__file__))


async def query_wfs(url: HttpUrl, layer: str, geom_field: str, geometry: ogr.Geometry, epsg: int, timeout: int = 30) -> tuple[int, str]:
    gml_str = geometry.ExportToGML(['FORMAT=GML3'])
    request_xml = __create_wfs_request_xml(layer, geom_field, gml_str, epsg)    

    return await __query_wfs(url, request_xml, timeout)


def __create_wfs_request_xml(layer: str, geom_field: str, gml_str: str, epsg: int) -> str:
    file_path = path.join(__DIR_PATH, 'wfs_request.xml.txt')

    with open(file_path, 'r') as file:
        file_text = file.read()

    return file_text.format(layer=layer,  geom_field=geom_field, geometry=gml_str, epsg=epsg).encode('utf-8')


async def __query_wfs(url: HttpUrl, xml_body: str, timeout: int) -> tuple[int, str]:
    url = f'{url}?service=WFS&version=2.0.0'
    headers = {'Content-Type': 'application/xml'}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=xml_body, headers=headers, timeout=timeout) as response:
                if response.status != 200:
                    return response.status, None

                return 200, await response.text()
    except asyncio.TimeoutError:
        return 408, None
    except:
        return 500, None
