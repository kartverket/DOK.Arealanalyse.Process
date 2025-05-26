from typing import List, Dict, Tuple
import aiohttp
from lxml import etree as ET
from osgeo import ogr, osr
from ..drivers.wfs import query_wfs

_WFS_URL = 'https://wfs.geonorge.no/skwms1/wfs.administrative_enheter'


async def get_municipality(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str]:
    municipality = await _get_municipality_from_rest_api(geometry, epsg)

    if municipality is not None:
        return municipality

    return await _get_municipality_from_wfs(geometry, epsg)


async def _get_municipality_from_rest_api(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str]:
    centroid: ogr.Geometry = geometry.Centroid()
    point: List[float] = centroid.GetPoint(0)

    return await _fetch_municipality(point[0], point[1], epsg)


async def _get_municipality_from_wfs(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str]:
    centroid: ogr.Geometry = geometry.Centroid()
    spatial_ref: osr.SpatialReference = geometry.GetSpatialReference()
    centroid.AssignSpatialReference(spatial_ref)

    _, response = await query_wfs(_WFS_URL, 'Kommune', 'område', centroid, epsg, 5)

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


async def _fetch_municipality(x: float, y: float, epsg: int) -> Tuple[str, str]:
    try:
        url = f'https://api.kartverket.no/kommuneinfo/v1/punkt?nord={y}&ost={x}&koordsys={epsg}&filtrer=kommunenummer,kommunenavn'

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status != 200:
                    return None

                json: Dict = await response.json()

                municipality_number = json.get('kommunenummer', None)
                municipality_name = json.get('kommunenavn', None)

                return municipality_number, municipality_name
    except:
        return None


__all__ = ['get_municipality']