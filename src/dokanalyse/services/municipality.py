from io import BytesIO
from typing import Any, Dict, List, Tuple
import structlog
from structlog.stdlib import BoundLogger
from lxml import etree as ET
from osgeo import ogr, osr
from ..adapters.wfs import query_wfs
from ..utils.http_context import get_session

_WFS_URL = 'https://wfs.geonorge.no/skwms1/wfs.administrative_enheter'

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_municipality(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str] | None:
    municipality = await _get_municipality_from_rest_api(geometry, epsg)

    if municipality is not None:
        return municipality

    return await _get_municipality_from_wfs(geometry, epsg)


async def _get_municipality_from_rest_api(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str] | None:
    centroid: ogr.Geometry = geometry.Centroid()
    point: List[float] = centroid.GetPoint(0)

    return await _fetch_municipality(point[0], point[1], epsg)


async def _get_municipality_from_wfs(geometry: ogr.Geometry, epsg: int) -> Tuple[str, str] | None:
    centroid: ogr.Geometry = geometry.Centroid()
    spatial_ref: osr.SpatialReference = geometry.GetSpatialReference()
    centroid.AssignSpatialReference(spatial_ref)

    _, response = await query_wfs(_WFS_URL, 'Kommune', 'område', centroid, epsg)

    if response is None:
        return None

    ns = {'wfs': 'http://www.opengis.net/wfs/2.0',
          'app': 'https://skjema.geonorge.no/SOSI/produktspesifikasjon/AdmEnheter/20240101'}

    bytes_io = BytesIO(response)
    root = ET.parse(bytes_io)

    municipality_number = root.findtext(
        './/wfs:member/*/app:kommunenummer', namespaces=ns)
    municipality_name = root.findtext(
        './/wfs:member/*/app:kommunenavn', namespaces=ns)

    if municipality_number is None or municipality_name is None:
        return None

    return municipality_number, municipality_name


async def _fetch_municipality(x: float, y: float, epsg: int) -> Tuple[str, str] | None:
    # autopep8: off
    url = f'https://api.kartverket.no/kommuneinfo/v1/punkt?nord={y}&ost={x}&koordsys={epsg}&filtrer=kommunenummer,kommunenavn'
    # autopep8: on

    try:
        async with get_session().get(url) as response:
            if response.status != 200:
                return None

            data: Dict[str, str] = await response.json()
            municipality_number = data['kommunenummer']
            municipality_name = data['kommunenavn']

            return municipality_number, municipality_name
    except Exception as err:
        _logger.error('Getting municipality failed', url=url, error=str(err))
        return None


__all__ = ['get_municipality']
