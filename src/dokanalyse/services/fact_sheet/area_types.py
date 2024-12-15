from typing import Dict
from osgeo import ogr
from ..codelist import get_codelist
from ..kartkatalog import get_kartkatalog_metadata
from ...models.fact_part import FactPart
from ...utils.constants import AR5_FGDB_PATH

_DATASET_ID = '166382b4-82d6-4ea9-a68e-6fd0c87bf788'
_LAYER_NAME = 'fkb_ar5_omrade'

async def get_area_types(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart:
    dataset = await get_kartkatalog_metadata(_DATASET_ID)
    data = await _get_data(geometry)
        
    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


async def _get_data(geometry: ogr.Geometry) -> Dict:
    driver: ogr.Driver = ogr.GetDriverByName('OpenFileGDB')
    data_source: ogr.DataSource = driver.Open(AR5_FGDB_PATH, 0)
    layer: ogr.Layer = data_source.GetLayerByName(_LAYER_NAME)
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


async def _map_area_types(area_types: Dict) -> Dict:
    codelist = await get_codelist('arealressurs_arealtype')
    mapped = []

    for entry in codelist:
        label = entry['label']
        area: float = next((value for key, value in area_types.items()
                            if key == entry['value']), None)
        data = {'areaType': label}

        if area is not None:
            data['area'] = round(area, 2)
        else:
            data['area'] = 0.00

        mapped.append(data)

    return sorted(mapped, key=lambda item: item['areaType'])


__all__ = ['get_area_types']