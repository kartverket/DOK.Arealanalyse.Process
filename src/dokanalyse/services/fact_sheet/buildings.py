import logging
import time
from uuid import UUID
from collections import Counter
from typing import List
from lxml import etree as ET
from osgeo import ogr
from ...drivers.wfs import query_wfs
from ...utils.helpers.common import parse_string
from ...services.kartkatalog import get_kartkatalog_metadata
from ...models.fact_part import FactPart

_LOGGER = logging.getLogger(__name__)

_METADATA_ID = UUID('24d7e9d1-87f6-45a0-b38e-3447f8d7f9a1')
_LAYER_NAME = 'Bygning'
_WFS_URL = 'https://wfs.geonorge.no/skwms1/wfs.matrikkelen-bygningspunkt'
_TIMEOUT = 10

_BUILDING_CATEGORIES = {
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


async def get_buildings(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart:
    start = time.time()
    dataset = await get_kartkatalog_metadata(_METADATA_ID)
    data = await _get_data(geometry, epsg)
    end = time.time()

    # autopep8: off
    _LOGGER.info(f'Fact sheet: Got buildings from Matrikkel WFS: {round(end - start, 2)} sec.')
    # autopep8: on

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


async def _get_data(geometry: ogr.Geometry, epsg: int) -> List[dict]:
    _, response = await query_wfs(_WFS_URL, _LAYER_NAME, 'representasjonspunkt', geometry, epsg, _TIMEOUT)

    if response is None:
        return None

    root = ET.fromstring(bytes(response, encoding='utf-8'))
    path = '//*[local-name() = "bygningstype"]'
    elems = root.xpath(path)
    categories = []

    for elem in elems:
        building_type = parse_string(elem.text)
        category = _get_building_category(building_type)

        if category is not None:
            categories.append(category)

    counted = Counter(categories)
    result: List[dict] = []

    for _, value in _BUILDING_CATEGORIES.items():
        count = counted.get(value, 0)

        result.append({
            'category': value,
            'count': count
        })

    return result


def _get_building_category(building_type: int) -> str:
    for range, category in _BUILDING_CATEGORIES.items():
        if range[0] <= building_type <= range[1]:
            return category

    return None


__all__ = ['get_buildings']
