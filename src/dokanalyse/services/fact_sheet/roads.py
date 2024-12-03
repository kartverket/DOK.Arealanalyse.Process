import json
from typing import List
from osgeo import ogr
from ..codelist import get_codelist
from ...http_clients.ogc_api import query_ogc_api
from ...services.kartkatalog import get_kartkatalog_metadata
from ...utils.helpers.geometry import geometry_from_json
from ...utils.helpers.common import from_camel_case
from ...models.fact_part import FactPart

__DATASET_ID = '900206a8-686f-4591-9394-327eb02d0899'
__LAYER_NAME = 'veglenke'
__BASE_URL = 'https://ogcapitest.kartverket.no/rest/services/forenklet_elveg_2_0/collections'


async def get_roads(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart:
    dataset = await get_kartkatalog_metadata(__DATASET_ID)
    data = await __get_data(geometry, epsg)

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {__LAYER_NAME}'], data)


async def __get_data(geometry: ogr.Geometry, epsg: int) -> List[dict]:
    _, response = await query_ogc_api(__BASE_URL, __LAYER_NAME, 'senterlinje', geometry, epsg, epsg)

    if response is None:
        return None

    return await __map_response(response)


async def __map_response(response: dict) -> List[dict]:
    features = response.get('features', [])
    road_categories = await get_codelist('vegkategori')
    road_types = {}

    for feature in features:
        json_str = json.dumps(feature['geometry'])
        geometry = geometry_from_json(json_str)
        props = feature.get('properties')
        road_type = props.get('typeVeg')

        if road_type == 'enkelBilveg':
            road_category = props.get('vegsystemreferanse', {}).get(
                'vegsystem', {}).get('vegkategori')

            if road_category is not None:
                entry = next(
                    (rc for rc in road_categories if rc['value'] == road_category), {})
                road_type = entry.get('label', road_type)

        if road_type in road_types:
            road_types[road_type] += geometry.Length()
        else:
            road_types[road_type] = geometry.Length()

    result: List[dict] = []

    for key, value in road_types.items():
        result.append({
            'roadType': from_camel_case(key),
            'length': round(value, 2)
        })

    return result
