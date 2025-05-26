import json
from io import BytesIO
from typing import List, Tuple, Dict, Union
from lxml import etree as ET
from osgeo import ogr
from ..models.config import CoverageService, CoverageGeoJson, CoverageGeoPackage
from ..drivers.wfs import query_wfs
from ..drivers.arcgis import query_arcgis
from ..drivers.geojson import query_geojson
from ..drivers.geopackage import query_geopackage
from ..utils.helpers.common import xpath_select_one, parse_string
from ..utils.helpers.geometry import geometry_from_gml, geometry_from_json


async def get_values_from_wfs(config: CoverageService, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    _, response = await query_wfs(config.url, config.layer, config.geom_field, geometry, epsg)

    if response is None:
        return [], 0, []

    source = BytesIO(response.encode('utf-8'))
    context = ET.iterparse(source, huge_tree=True)

    prop_path = f'.//*[local-name() = "{config.property}"]/text()'
    geom_path = f'.//*[local-name() = "{config.geom_field}"]/*'
    values: List[str] = []
    data: List[Dict] = []
    feature_geoms: List[ogr.Geometry] = []
    hit_area_percent = 0

    for _, elem in context:
        localname = ET.QName(elem).localname

        if localname == 'member':
            value = xpath_select_one(elem, prop_path)
            values.append(value)

            if value == 'ikkeKartlagt':
                geom_element = xpath_select_one(elem, geom_path)
                gml_str = ET.tostring(geom_element, encoding='unicode')
                feature_geom = geometry_from_gml(gml_str)

                if feature_geom:
                    feature_geoms.append(feature_geom)

            if len(config.properties) > 0:
                props = _map_wfs_properties(elem, config.properties)
                data.append(props)

    if len(feature_geoms) > 0:
        hit_area_percent = _get_hit_area_percent(geometry, feature_geoms)

    distinct_values = list(set(values))

    return distinct_values, hit_area_percent, data


async def get_values_from_arcgis(config: CoverageService, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    _, response = await query_arcgis(config.url, config.layer, None, geometry, epsg)

    if response is None:
        return [], 0, []

    features: List[Dict] = response.get('features')

    if len(features) == 0:
        return [], 0, []

    values: List[str] = []
    data: List[Dict] = []

    for feature in features:
        value = feature.get('properties').get(config.property)
        values.append(value)

        if len(config.properties) > 0:
            props = _map_geojson_properties(feature, config.properties)
            data.append(props)

    distinct_values = list(set(values))

    return distinct_values, 0, data


async def get_values_from_geojson(config: Union[CoverageGeoJson, CoverageGeoPackage], geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    if isinstance(config, CoverageGeoJson):
        response = await query_geojson(config.url, config.filter, geometry, epsg)
    else:
        response = await query_geopackage(config.url, config.filter, geometry, epsg)

    if response is None:
        return [], 0, []

    features: List[Dict] = response.get('features')

    if len(features) == 0:
        return [], 0, []

    values: List[str] = []
    data: List[Dict] = []
    feature_geoms: List[ogr.Geometry] = []
    hit_area_percent = 0

    for feature in features:
        value = feature.get('properties').get(config.property)
        values.append(value)

        if value in ['ikkeKartlagt', 'Ikke kartlagt']:
            feature_geom = feature.get('geometry')
            json_str = json.dumps(feature_geom)
            geom = geometry_from_json(json_str)
            feature_geoms.append(geom)

        if len(config.properties) > 0:
            props = _map_geojson_properties(feature, config.properties)
            data.append(props)

    if len(feature_geoms) > 0:
        hit_area_percent = _get_hit_area_percent(geometry, feature_geoms)

    distinct_values = list(set(values))

    return distinct_values, hit_area_percent, data


def _get_hit_area_percent(geometry: ogr.Geometry, feature_geometries: List[ogr.Geometry]) -> float:
    geom_area: float = geometry.GetArea()
    hit_area: float = 0

    for geom in feature_geometries:
        intersection: ogr.Geometry = geom.Intersection(geometry)

        if intersection is None:
            continue

        area: float = intersection.GetArea()
        hit_area += area

    percent = (hit_area / geom_area) * 100

    return round(percent, 2)


def _map_wfs_properties(member: ET._Element, mappings: List[str]) -> Dict:
    props = {}

    for mapping in mappings:
        path = f'.//*[local-name() = "{mapping}"]/text()'
        value = xpath_select_one(member, path)

        if value:
            prop_name = mapping
            props[prop_name] = parse_string(value)

    return props


def _map_geojson_properties(feature: Dict, mappings: List[str]) -> Dict:
    props = {}
    feature_props: Dict = feature['properties']

    for mapping in mappings:
        props[mapping] = feature_props.get(mapping)

    return props


__all__ = ['get_values_from_wfs',
           'get_values_from_arcgis', 'get_values_from_geojson']
