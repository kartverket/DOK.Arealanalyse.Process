import json
from osgeo import ogr, osr
from math import pi
from re import search
from shapely import wkt
from shapely.wkt import dumps
from ..constants import DEFAULT_EPSG, WGS84_EPSG

EARTH_RADIUS = 6371008.8


def geometry_from_gml(gml_str: str) -> ogr.Geometry:
    try:
        return ogr.CreateGeometryFromGML(gml_str)
    except:
        return None


def geometry_from_json(json_str: str) -> ogr.Geometry:
    try:
        return ogr.CreateGeometryFromJson(json_str)
    except:
        return None


def geometry_to_wkt(geometry: ogr.Geometry, epsg: int) -> str:
    wkt_str = geometry.ExportToWkt()
    geometry = wkt.loads(wkt_str)
    coord_precision = 6 if epsg == WGS84_EPSG else 2

    return dumps(geometry, trim=True, rounding_precision=coord_precision)


def geometry_to_arcgis_geom(geometry: ogr.Geometry, epsg: int) -> str:
    if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
        out_geom = ogr.ForceToPolygon(geometry)
    else:
        out_geom = geometry

    coord_precision = 6 if epsg == WGS84_EPSG else 2
    geojson = out_geom.ExportToJson(
        [f'COORDINATE_PRECISION={coord_precision}'])
    obj = json.loads(geojson)

    arcgis_geom = {
        'rings': obj['coordinates'],
        'spatialReference': {
            'wkid': epsg
        }
    }

    return json.dumps(arcgis_geom)


def create_input_geometry(geo_json: dict) -> ogr.Geometry:
    epsg = get_epsg(geo_json)
    geometry = ogr.CreateGeometryFromJson(str(geo_json))

    if epsg != DEFAULT_EPSG:
        return transform_geometry(geometry, epsg, DEFAULT_EPSG)

    return geometry


def create_buffered_geometry(geometry: ogr.Geometry, distance: int, epsg: int) -> ogr.Geometry:
    computed_buffer = length_to_degrees(
        distance) if epsg is None or epsg == WGS84_EPSG else distance

    return geometry.Buffer(computed_buffer, 10)


def transform_geometry(geometry: ogr.Geometry, src_epsg: int, dest_epsg: int) -> ogr.Geometry:
    source = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)
    source.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    target = osr.SpatialReference()
    target.ImportFromEPSG(dest_epsg)

    transform = osr.CoordinateTransformation(source, target)
    clone: ogr.Geometry = geometry.Clone()
    clone.Transform(transform)

    return clone


def length_to_degrees(distance: float) -> float:
    radians = distance / EARTH_RADIUS
    degrees = radians % (2 * pi)

    return degrees * 180 / pi


def create_run_on_input_geometry_json(geometry: ogr.Geometry, epsg: int, orig_epsg: int) -> dict:
    geom = geometry

    if epsg != orig_epsg:
        geom = transform_geometry(geometry, epsg, orig_epsg)

    coord_precision = 6 if orig_epsg == WGS84_EPSG else 2
    geo_json = json.loads(geom.ExportToJson(
        [f'COORDINATE_PRECISION={coord_precision}']))
    add_geojson_crs(geo_json, epsg)

    return geo_json


def get_epsg(geo_json: dict) -> int:
    crs = geo_json.get('crs', {}).get('properties', {}).get('name')

    if crs is None:
        return WGS84_EPSG

    regex = r'^(http:\/\/www\.opengis\.net\/def\/crs\/EPSG\/0\/|^urn:ogc:def:crs:EPSG::|^EPSG:)(?P<epsg>\d+)$'
    matches = search(regex, crs)

    if matches:
        return int(matches.group('epsg'))

    return WGS84_EPSG


def add_geojson_crs(geojson: dict, epsg: int) -> None:
    if epsg is None or epsg == WGS84_EPSG:
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': 'urn:ogc:def:crs:EPSG::' + str(epsg)
        }
    }
