import time
import asyncio
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


async def get_area_types(geometry: ogr.Geometry, epsg: int, orig_epsg: int | None, buffer: int) -> FactPart | None:
    if not AR5_FGDB_PATH:
        return None

    start = time.time()
    dataset = await get_kartkatalog_metadata(_metadata_id)
    area_types = await asyncio.to_thread(_get_area_types, geometry, AR5_FGDB_PATH)

    data = {
        'inputArea': round(geometry.GetArea(), 2),
        'areaTypes': await _map_area_types(area_types)
    }

    end = time.time()

    _logger.info('Fact sheet: Got area types from FKB AR5', duration=round(end - start, 2))

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


def _get_area_types(geometry: ogr.Geometry, fgdb_path: str) -> Dict[str, float]:
    """Sums the intersecting area per AR5 area type. Blocking."""
    driver: gdal.Driver = ogr.GetDriverByName('OpenFileGDB')
    dataset: gdal.Dataset | None = driver.Open(fgdb_path)

    if dataset is None:
        raise ValueError(f'Could not open AR5 file geodatabase: {fgdb_path}')

    layer: ogr.Layer | None = dataset.GetLayerByName(_LAYER_NAME)

    if layer is None:
        raise ValueError(f'Layer "{_LAYER_NAME}" not found in AR5 file geodatabase')

    layer.SetSpatialFilter(0, geometry)
    area_types: Dict[str, float] = {}

    feature: ogr.Feature
    for feature in layer:
        geom: ogr.Geometry | None = feature.GetGeometryRef()

        if geom is None:
            continue

        intersection: ogr.Geometry | None = geometry.Intersection(geom)

        if intersection is None:
            continue

        area_type = feature.GetField('arealtype')
        area_types[area_type] = area_types.get(area_type, 0) + intersection.GetArea()

    return area_types


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
