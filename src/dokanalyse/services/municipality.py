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
    sr: osr.SpatialReference = geometry.GetSpatialReference()
    centroid.AssignSpatialReference(sr)

    _, response = await query_wfs(_WFS_URL, 'Kommune', 'område', centroid, epsg)

    if response is None:
        return None

    source = BytesIO(response)
    context = ET.iterparse(
        source, events=['end'], tag='{*}Kommune', huge_tree=True)

    municipality_number: str | None = None
    municipality_name: str | None = None

    for _, elem in context:
        municipality_number = elem.findtext('./{*}kommunenummer')
        municipality_name = elem.findtext('./{*}kommunenavn')

        elem.clear()
        break

    del context

    if municipality_number and municipality_name:
        return municipality_number, municipality_name

    return None


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
