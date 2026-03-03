import time
from uuid import UUID
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from osgeo import gdal, ogr
from ..codelist import get_codelist
from ..kartkatalog import get_kartkatalog_metadata
from ...models.fact_part import FactPart
from ...constants import AR5_FGDB_PATH

_LAYER_NAME = 'fkb_ar5_omrade'

_logger: BoundLogger = structlog.get_logger(__name__)

_metadata_id = UUID('166382b4-82d6-4ea9-a68e-6fd0c87bf788')


async def get_area_types(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart | None:
    if not AR5_FGDB_PATH:
        return None

    start = time.time()
    dataset = await get_kartkatalog_metadata(_metadata_id)
    data = await _get_data(geometry)
    end = time.time()

    _logger.info('Fact sheet: Got area types from FKB AR5', duration=round(end - start, 2))

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


async def _get_data(geometry: ogr.Geometry) -> Dict[str, Any]:
    driver: gdal.Driver = ogr.GetDriverByName('OpenFileGDB')
    dataset: gdal.Dataset = driver.Open(AR5_FGDB_PATH)
    layer: ogr.Layer = dataset.GetLayerByName(_LAYER_NAME)
    layer.SetSpatialFilter(0, geometry)

    input_area = geometry.GetArea()
    area_types = {}

    feature: ogr.Feature
    for feature in layer:
        area_type = feature.GetField('arealtype')
        geom: ogr.Geometry = feature.GetGeometryRef()
        intersection: ogr.Geometry = geometry.Intersection(geom)
        geom_area: float = intersection.GetArea()

        if area_type in area_types:
            area_types[area_type] += geom_area
        else:
            area_types[area_type] = geom_area

    return {
        'inputArea': round(input_area, 2),
        'areaTypes': await _map_area_types(area_types)
    }


async def _map_area_types(area_types: Dict[str, Any]) -> List[Dict[str, Any]]:
    codelist = await get_codelist('arealressurs_arealtype')
    mapped = []

    for entry in codelist:
        label = entry['label']
        area: float | None = next((value for key, value in area_types.items()
                            if key == entry['value']), None)
        data = {'areaType': label}

        if area is not None:
            data['area'] = round(area, 2)
        else:
            data['area'] = 0.00

        mapped.append(data)

    return sorted(mapped, key=lambda item: item['areaType'])


__all__ = ['get_area_types']
