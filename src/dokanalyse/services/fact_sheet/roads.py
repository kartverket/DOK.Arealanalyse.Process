import json
from typing import List, Dict
from osgeo import ogr
from ..codelist import get_codelist
from ...http_clients.ogc_api import query_ogc_api
from ...services.kartkatalog import get_kartkatalog_metadata
from ...utils.helpers.geometry import geometry_from_json
from ...utils.helpers.common import from_camel_case
from ...models.fact_part import FactPart

_DATASET_ID = '900206a8-686f-4591-9394-327eb02d0899'
_LAYER_NAME = 'veglenke'
_BASE_URL = 'https://ogcapitest.kartverket.no/rest/services/forenklet_elveg_2_0/collections'


async def get_roads(geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> FactPart:
    dataset = await get_kartkatalog_metadata(_DATASET_ID)
    data = await _get_data(geometry, epsg)

    return FactPart(geometry, epsg, orig_epsg, buffer, dataset, [f'intersect {_LAYER_NAME}'], data)


async def _get_data(geometry: ogr.Geometry, epsg: int) -> List[Dict]:
    _, response = await query_ogc_api(_BASE_URL, _LAYER_NAME, 'senterlinje', geometry, epsg, epsg)

    if response is None:
        return None

    return await _map_response(response)


async def _map_response(response: Dict) -> List[Dict]:
    features: List[Dict] = response.get('features', [])
    road_categories = await get_codelist('vegkategori')
    road_types = {}

    for feature in features:
        json_str = json.dumps(feature['geometry'])
        geometry = geometry_from_json(json_str)
        props: Dict = feature.get('properties')
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

    result: List[Dict] = []

    for key, value in road_types.items():
        result.append({
            'roadType': from_camel_case(key),
            'length': round(value, 2)
        })

    return result


__all__ = ['get_roads']