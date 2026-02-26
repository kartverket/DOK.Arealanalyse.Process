from typing import List, Tuple, Dict, Union
from osgeo import ogr
from .wfs_response import CoverageWfsResponseParser
from ..models.config import CoverageService, CoverageGeoJson, CoverageGeoPackage
from ..adapters.wfs import query_wfs
from ..adapters.arcgis import query_arcgis
from ..adapters.geojson import query_geojson
from ..adapters.geopackage import query_geopackage


async def get_values_from_wfs(config: CoverageService, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    _, response = await query_wfs(config.url, config.layer, config.geom_field, geometry, epsg)

    if response is None:
        return [], 0, []

    parser = CoverageWfsResponseParser(config)
    values, feature_geoms, data = parser.parse(response)

    hit_area_percent = _get_hit_area_percent(geometry, feature_geoms) if feature_geoms else 0
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
        props: Dict = feature['properties']
        value = props.get(config.property)
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
        props: Dict = feature['properties']
        value = props.get(config.property)
        values.append(value)

        if value in ['ikkeKartlagt', 'Ikke kartlagt']:
            feature_geom = feature.get('geometry')
            feature_geoms.append(feature_geom)

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


def _map_geojson_properties(feature: Dict, mappings: List[str]) -> Dict:
    props = {}
    feature_props: Dict = feature['properties']

    for mapping in mappings:
        props[mapping] = feature_props.get(mapping)

    return props


__all__ = ['get_values_from_wfs',
           'get_values_from_arcgis', 'get_values_from_geojson']
