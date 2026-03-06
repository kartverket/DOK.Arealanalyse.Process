import time
from io import BytesIO
from uuid import UUID
from collections import Counter
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from lxml import etree as ET
from osgeo import ogr
from ...adapters.wfs import query_wfs
from ...services.kartkatalog import get_kartkatalog_metadata
from ...models.fact_part import FactPart

_WFS_URL = 'https://wfs.geonorge.no/skwms1/wfs.matrikkelen-bygningspunkt'
_LAYER_NAME = 'Bygning'

_metadata_id = UUID('24d7e9d1-87f6-45a0-b38e-3447f8d7f9a1')

_building_categories = {
    (100, 159): 'Bolig',
    (160, 180): 'Fritidsbolig - hytte',
    (200, 299): 'Industri og lagerbygning',
    (300, 399): 'Kontor- og forretningsbygning',
    (400, 499): 'Samferdsels- og kommunikasjonsbygning',
    (500, 599): 'Hotell og restaurantbygning',
    (600, 699): 'Skole-, kultur-, idrett-, forskningsbygning',
    (700, 799): 'Helse- og omsorgsbygning',
    (800, 899): 'Fengsel, beredskapsbygning, mv.',
}

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_buildings(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart:
    start = time.time()
    dataset = await get_kartkatalog_metadata(_metadata_id)
    data = await _get_data(geometry, epsg)
    end = time.time()

    _logger.info('Fact sheet: Got buildings from Matrikkel WFS', duration=round(end - start, 2))

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


async def _get_data(geometry: ogr.Geometry, epsg: int) -> List[Dict[str, Any]]:
    _, response = await query_wfs(_WFS_URL, _LAYER_NAME, 'representasjonspunkt', geometry, epsg)

    if response is None:
        return []
    
    source = BytesIO(response)
    context = ET.iterparse(
        source, events=['end'], tag='{*}Bygning', huge_tree=True)        
    categories = []

    for _, elem in context:        
        building_type = elem.findtext('./{*}bygningstype')

        if not building_type:
            elem.clear()
            continue
    
        category = _get_building_category(int(building_type))

        if category:
            categories.append(category)

        elem.clear()

    del context

    counted = Counter(categories)
    result: List[Dict[str, Any]] = []

    for _, value in _building_categories.items():
        count = counted.get(value, 0)

        result.append({
            'category': value,
            'count': count
        })

    return result


def _get_building_category(building_type: int) -> str | None:
    for range, category in _building_categories.items():
        if range[0] <= building_type <= range[1]:
            return category

    return None


__all__ = ['get_buildings']
