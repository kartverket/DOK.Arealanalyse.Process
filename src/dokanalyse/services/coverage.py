from typing import Any, Dict, List, Literal, Tuple
from osgeo import ogr
from pydash import get as pydash_get
from .wfs_response import CoverageWfsResponseParser
from ..models.config import CoverageService, CoverageGeoJson, CoverageGeoPackage, CoverageWfs
from ..adapters.wfs import query_wfs
from ..adapters.arcgis import query_arcgis
from ..adapters.geofile import query_geofile
from ..utils.helpers.common import objectify_properties


async def get_values_from_wfs(config: CoverageWfs, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    _, response = await query_wfs(config.url, config.layer, config.geom_field, geometry, epsg)

    if response is None:
        return [], 0, []

    parser = CoverageWfsResponseParser(config)
    values, feature_geoms, data = parser.parse(response)

    hit_area_percent = _get_hit_area_percent(
        geometry, feature_geoms) if feature_geoms else 0
    distinct_values = list(set(values))

    return distinct_values, hit_area_percent, data


async def get_values_from_arcgis(config: CoverageService, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    _, response = await query_arcgis(config.url, config.layer, None, geometry, epsg)

    if response is None:
        return [], 0, []

    features: List[Dict[str, Any]] = response.get('features', [])

    if not features:
        return [], 0, []

    values: List[str] = []
    data: List[Dict] = []

    for feature in features:
        props: Dict[str, Any] = feature['properties']
        value: str | None = props.get(config.property)

        if not value:
            continue

        values.append(value)

        if len(config.properties) > 0:
            props = _map_properties(feature, config.properties)
            data.append(props)

    distinct_values = list(set(values))

    return distinct_values, 0, data


async def get_values_from_geofile(
    config: CoverageGeoJson | CoverageGeoPackage,
    geometry: ogr.Geometry, epsg: int
) -> Tuple[List[str], float, List[Dict]]:
    driver_name = _get_gdal_driver_name(config)
    response = await query_geofile(config.url, driver_name, config.filter, geometry, epsg)

    if response is None:
        return [], 0, []

    features: List[Dict[str, Any]] = response.get('features', [])

    if not features:
        return [], 0, []

    values: List[str] = []
    data: List[Dict] = []
    feature_geoms: List[ogr.Geometry] = []
    hit_area_percent = 0

    for feature in features:
        props: Dict[str, Any] = feature['properties']
        value = props.get(config.property)

        if not value:
            continue

        values.append(value)

        if value in ['ikkeKartlagt', 'Ikke kartlagt']:
            feature_geom: ogr.Geometry = feature['geometry']
            feature_geoms.append(feature_geom)

        if len(config.properties) > 0:
            props = _map_properties(feature, config.properties)
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


def _map_properties(feature: Dict[str, Any], properties: List[str] | Literal['*']) -> Dict[str, Any]:
    if isinstance(properties, str):
        return feature['properties']

    props = {}

    for prop_name in properties:
        value = pydash_get(feature['properties'], prop_name, None)
        props[prop_name] = value

    return objectify_properties(props)


def _get_gdal_driver_name(config: CoverageGeoJson | CoverageGeoPackage) -> Literal['GeoJSON', 'GPKG']:
    if isinstance(config, CoverageGeoJson):
        return 'GeoJSON'

    return 'GPKG'


__all__ = ['get_values_from_wfs',
           'get_values_from_arcgis', 'get_values_from_geofile']
