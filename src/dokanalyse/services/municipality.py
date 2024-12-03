import aiohttp
from lxml import etree as ET
from osgeo import ogr, osr
from ..http_clients.wfs import query_wfs

__WFS_URL = 'https://wfs.geonorge.no/skwms1/wfs.administrative_enheter'


async def get_municipality(geometry: ogr.Geometry, epsg: int) -> tuple[str, str]:
    return await __get_municipality_from_rest_api(geometry, epsg)


async def __get_municipality_from_rest_api(geometry: ogr.Geometry, epsg: int) -> tuple[str, str]:
    centroid = geometry.Centroid()
    point = centroid.GetPoint(0)

    return await __fetch_municipality(point[0], point[1], epsg)


async def __get_municipality_from_wfs(geometry: ogr.Geometry, epsg: int) -> tuple[str, str]:
    centroid: ogr.Geometry = geometry.Centroid()
    spatial_ref: osr.SpatialReference = geometry.GetSpatialReference()
    centroid.AssignSpatialReference(spatial_ref)

    _, response = await query_wfs(__WFS_URL, 'Kommune', 'område', centroid, epsg)

    if response is None:
        return None

    ns = {'wfs': 'http://www.opengis.net/wfs/2.0',
          'app': 'https://skjema.geonorge.no/SOSI/produktspesifikasjon/AdmEnheter/20240101'}

    root = ET.fromstring(bytes(response, encoding='utf-8'))
    municipality_number = root.findtext(
        './/wfs:member/*/app:kommunenummer', namespaces=ns)
    municipality_name = root.findtext(
        './/wfs:member/*/app:kommunenavn', namespaces=ns)

    return municipality_number, municipality_name


async def __fetch_municipality(x: float, y: float, epsg: int) -> tuple[str, str]:
    try:
        url = f'https://api.kartverket.no/kommuneinfo/v1/punkt?nord={y}&ost={x}&koordsys={epsg}&filtrer=kommunenummer,kommunenavn'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                json = await response.json()

                municipality_number = json.get('kommunenummer', None)
                municipality_name = json.get('kommunenavn', None)

                return municipality_number, municipality_name
    except:
        return None
