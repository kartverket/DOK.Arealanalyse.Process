import json
import re
from math import pi
from typing import Any, Dict, List
from osgeo import ogr, osr
import structlog
from structlog.stdlib import BoundLogger
from shapely import wkt
from shapely.wkt import dumps
from ...constants import DEFAULT_EPSG, WGS84_EPSG

_EARTH_RADIUS = 6371008.8

_logger: BoundLogger = structlog.get_logger(__name__)

_crs_regex = re.compile(r'^(http:\/\/www\.opengis\.net\/def\/crs\/EPSG\/0\/|^urn:ogc:def:crs:EPSG::|^EPSG:)(?P<epsg>\d+)$')


def geometry_from_gml(gml_str: str) -> ogr.Geometry | None:
    try:
        return ogr.CreateGeometryFromGML(gml_str)
    except Exception as err:
        _logger.error('Geometry from GML failed', gml=gml_str, error=err)
        return None


def geometry_from_json(json_str: str) -> ogr.Geometry | None:
    try:
        return ogr.CreateGeometryFromJson(json_str)
    except Exception as err:
        _logger.error('Geometry from GeoJSON failed', json=json_str, error=err)
        return None


def geometry_to_wkt(geometry: ogr.Geometry, epsg: int) -> str:
    wkt_str = geometry.ExportToWkt()
    geometry = wkt.loads(wkt_str)
    coord_precision = 2 if epsg != WGS84_EPSG else -1

    return dumps(geometry, trim=True, rounding_precision=coord_precision)


def geometry_to_arcgis_geom(geometry: ogr.Geometry, epsg: int) -> str:
    if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
        out_geom = ogr.ForceToPolygon(geometry)
    else:
        out_geom = geometry

    options = ['COORDINATE_PRECISION=2'] if epsg != WGS84_EPSG else []
    geojson = out_geom.ExportToJson(options)
    obj = json.loads(geojson)

    arcgis_geom = {
        'rings': obj['coordinates'],
        'spatialReference': {
            'wkid': epsg
        }
    }

    return json.dumps(arcgis_geom)


def create_input_geometry(geojson: Dict[str, Any]) -> ogr.Geometry:
    epsg = get_epsg_from_geojson(geojson)
    json_str = json.dumps(geojson)
    geometry = ogr.CreateGeometryFromJson(json_str)

    if epsg != DEFAULT_EPSG:
        return transform_geometry(geometry, epsg, DEFAULT_EPSG)

    return geometry


def create_buffered_geometry(geometry: ogr.Geometry, distance: int, epsg: int) -> ogr.Geometry:
    computed_buffer = length_to_degrees(
        distance) if epsg is None or epsg == WGS84_EPSG else distance

    return geometry.Buffer(computed_buffer, 10)


def create_feature_collection(features: List[Dict[str, Any]], epsg: int = 4326) -> Dict:
    feature_collection = {
        'type': 'FeatureCollection',
        'features': features
    }

    add_geojson_crs(feature_collection, epsg)

    return feature_collection


def create_feature(geometry: ogr.Geometry, properties: Dict[str, Any] = {}) -> Dict:
    json_str = geometry.ExportToJson()

    return {
        'type': 'Feature',
        'geometry': json.loads(json_str),
        'properties': properties
    }


def transform_geometry(geometry: ogr.Geometry, src_epsg: int, dest_epsg: int) -> ogr.Geometry:
    transform = get_coordinate_transformation(src_epsg, dest_epsg)
    clone: ogr.Geometry = geometry.Clone()
    clone.Transform(transform)

    return clone


def get_coordinate_transformation(src_epsg: int, target_epsg: int) -> osr.CoordinateTransformation:
    source: osr.SpatialReference = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)

    target: osr.SpatialReference = osr.SpatialReference()
    target.ImportFromEPSG(target_epsg)

    return osr.CoordinateTransformation(source, target)


def length_to_degrees(distance: float) -> float:
    radians = distance / _EARTH_RADIUS
    degrees = radians % (2 * pi)

    return degrees * 180 / pi


def create_run_on_input_geometry_json(geometry: ogr.Geometry, epsg: int, orig_epsg: int) -> Dict[str, Any]:
    geom = geometry

    if epsg != orig_epsg:
        geom = transform_geometry(geometry, epsg, orig_epsg)

    options = ['COORDINATE_PRECISION=2'] if orig_epsg != WGS84_EPSG else []
    geojson = json.loads(geom.ExportToJson(options))

    add_geojson_crs(geojson, epsg)

    return geojson


def get_epsg_from_geometry(geometry: ogr.Geometry) -> int | None:
    sr: osr.SpatialReference = geometry.GetSpatialReference()
    code = sr.GetAuthorityCode(None)

    return int(code) if code else None


def get_epsg_from_crs(crs: str) -> int | None:
    sr = osr.SpatialReference()
    sr.SetFromUserInput(crs)
    code = sr.GetAuthorityCode(None)

    return int(code) if code else None


def get_epsg_from_geojson(geojson: Dict[str, Any]) -> int:
    crs = geojson.get('crs', {}).get('properties', {}).get('name')
    
    if crs is None:
        return WGS84_EPSG

    match = _crs_regex.search(crs)

    if match:
        return int(match.group('epsg'))

    return WGS84_EPSG


def add_geojson_crs(geojson: Dict[str, Any], epsg: int) -> None:
    if epsg is None or epsg == WGS84_EPSG:
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': 'urn:ogc:def:crs:EPSG::' + str(epsg)
        }
    }


__all__ = [
    'geometry_from_gml',
    'geometry_from_json',
    'geometry_to_wkt',
    'geometry_to_arcgis_geom',
    'create_input_geometry',
    'create_buffered_geometry',
    'create_feature_collection',
    'create_feature',
    'transform_geometry',
    'get_coordinate_transformation',
    'length_to_degrees',
    'create_run_on_input_geometry_json',
    'get_epsg_from_geometry',
    'get_epsg_from_crs',
    'get_epsg_from_geojson',
    'add_geojson_crs'
]
