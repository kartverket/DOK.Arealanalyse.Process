import json
import re
from math import pi
from typing import Any, Dict, List, Tuple
from osgeo import ogr, osr
import structlog
from structlog.stdlib import BoundLogger
from shapely import wkt
from shapely.wkt import dumps
from ...constants import DEFAULT_EPSG, WGS84_EPSG

_EARTH_RADIUS = 6371008.8

_logger: BoundLogger = structlog.get_logger(__name__)

_crs_regex = re.compile(
    r'^(http:\/\/www\.opengis\.net\/def\/crs\/EPSG\/0\/|^urn:ogc:def:crs:EPSG::|^EPSG:)(?P<epsg>\d+)$')


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
    wkt_geometry = wkt.loads(wkt_str)
    coord_precision = 2 if epsg != WGS84_EPSG else -1

    return dumps(wkt_geometry, trim=True, rounding_precision=coord_precision)


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


def envelope_to_polygon(envelope: Tuple[float, float, float, float]) -> ogr.Geometry:
    minx, maxx, miny, maxy = envelope
    ring = ogr.Geometry(ogr.wkbLinearRing)

    ring.AddPoint_2D(minx, miny)
    ring.AddPoint_2D(maxx, miny)
    ring.AddPoint_2D(maxx, maxy)
    ring.AddPoint_2D(minx, maxy)
    ring.AddPoint_2D(minx, miny)

    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)

    return polygon


def create_input_geometry(geojson: Dict[str, Any]) -> ogr.Geometry:
    epsg = get_epsg_from_geojson(geojson)
    json_str = json.dumps(geojson)

    try:
        geometry: ogr.Geometry | None = ogr.CreateGeometryFromJson(json_str)
    except Exception as err:
        raise ValueError(f'Invalid input geometry: {err}') from err

    if geometry is None or geometry.IsEmpty():
        raise ValueError('The input geometry is empty')

    if epsg != DEFAULT_EPSG:
        geometry = transform_geometry(geometry, epsg, DEFAULT_EPSG)

    if not geometry.IsValid():
        geometry = _make_valid_polygon(geometry)

    return geometry


def create_buffered_geometry(geometry: ogr.Geometry, distance: int, epsg: int | None) -> ogr.Geometry:
    computed_buffer = length_to_degrees(
        distance) if epsg is None or epsg == WGS84_EPSG else distance

    return geometry.Buffer(computed_buffer, 10)


def create_feature_collection(features: List[Dict[str, Any]], epsg: int | None = None) -> Dict:
    feature_collection = {
        'type': 'FeatureCollection',
        'features': features
    }

    add_geojson_crs(feature_collection, epsg)

    return feature_collection


def transform_geometry(
    geometry: ogr.Geometry,
    src_epsg: int | None,
    dest_epsg: int | None,
    traditional_axis_order: bool = True
) -> ogr.Geometry:
    """Transforms a geometry between two EPSG codes (None means WGS84).

    With `traditional_axis_order`, coordinates are always x/y (lon/lat), as
    in GeoJSON, ArcGIS JSON and data read by GDAL drivers. Without it, the
    axis order defined by the EPSG authority is used (e.g. lat/lon for
    EPSG:4326), as in GML with URN-style srsName.
    """
    transform = get_coordinate_transformation(
        src_epsg or WGS84_EPSG, dest_epsg or WGS84_EPSG, traditional_axis_order)
    clone: ogr.Geometry = geometry.Clone()
    clone.Transform(transform)

    return clone


def get_coordinate_transformation(
    src_epsg: int,
    target_epsg: int,
    traditional_axis_order: bool = True
) -> osr.CoordinateTransformation:
    source = create_spatial_reference(src_epsg, traditional_axis_order)
    target = create_spatial_reference(target_epsg, traditional_axis_order)

    return osr.CoordinateTransformation(source, target)


def create_spatial_reference(epsg: int, traditional_axis_order: bool = True) -> osr.SpatialReference:
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)

    if traditional_axis_order:
        sr.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    return sr


def length_to_degrees(distance: float) -> float:
    radians = distance / _EARTH_RADIUS
    degrees = radians % (2 * pi)

    return degrees * 180 / pi


def create_run_on_input_geometry_json(geometry: ogr.Geometry, epsg: int, orig_epsg: int | None) -> Dict[str, Any]:
    """Returns the geometry as GeoJSON in the CRS of the input geometry.

    `orig_epsg` is None when the input had no crs member (WGS84), in which
    case no crs member is added either.
    """
    geom = geometry

    if epsg != orig_epsg:
        geom = transform_geometry(geometry, epsg, orig_epsg)

    options = ['COORDINATE_PRECISION=2'] if _is_projected(orig_epsg) else []
    geojson = json.loads(geom.ExportToJson(options))

    add_geojson_crs(geojson, orig_epsg)

    return geojson


def get_epsg_from_geometry(geometry: ogr.Geometry) -> int | None:
    sr: osr.SpatialReference | None = geometry.GetSpatialReference()

    if sr is None:
        return None

    code = sr.GetAuthorityCode(None)

    return int(code) if code else None


def get_epsg_from_crs(crs: str) -> int | None:
    sr = osr.SpatialReference()
    sr.SetFromUserInput(crs)
    code = sr.GetAuthorityCode(None)

    return int(code) if code else None


def get_epsg_from_geojson(geojson: Dict[str, Any]) -> int | None:
    crs = geojson.get('crs', {}).get('properties', {}).get('name')

    if crs is None:
        return None

    match = _crs_regex.search(crs)

    if match:
        return int(match.group('epsg'))

    return None


def add_geojson_crs(geojson: Dict[str, Any], epsg: int | None) -> None:
    if epsg is None:
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': 'urn:ogc:def:crs:EPSG::' + str(epsg)
        }
    }


def _make_valid_polygon(geometry: ogr.Geometry) -> ogr.Geometry:
    """Repairs an invalid (e.g. self-intersecting) polygon, keeping only
    polygonal parts. Invalid geometries make GEOS operations fail."""
    valid: ogr.Geometry = geometry.MakeValid()
    sr = geometry.GetSpatialReference()

    if valid.GetGeometryType() not in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        multi = ogr.Geometry(ogr.wkbMultiPolygon)

        for i in range(valid.GetGeometryCount()):
            part: ogr.Geometry = valid.GetGeometryRef(i)

            if part.GetGeometryType() == ogr.wkbPolygon:
                multi.AddGeometry(part)
            elif part.GetGeometryType() == ogr.wkbMultiPolygon:
                for j in range(part.GetGeometryCount()):
                    multi.AddGeometry(part.GetGeometryRef(j))

        valid = multi

    if valid.IsEmpty():
        raise ValueError('The input geometry is invalid and could not be repaired')

    if sr is not None:
        valid.AssignSpatialReference(sr)

    _logger.warning('Invalid input geometry was repaired')

    return valid


def _is_projected(epsg: int | None) -> bool:
    if epsg is None:
        return False

    return bool(create_spatial_reference(epsg).IsProjected())


__all__ = [
    'geometry_from_gml',
    'geometry_from_json',
    'geometry_to_wkt',
    'geometry_to_arcgis_geom',
    'envelope_to_polygon',
    'create_input_geometry',
    'create_buffered_geometry',
    'create_feature_collection',
    'transform_geometry',
    'get_coordinate_transformation',
    'length_to_degrees',
    'create_run_on_input_geometry_json',
    'get_epsg_from_geometry',
    'get_epsg_from_crs',
    'get_epsg_from_geojson',
    'add_geojson_crs'
]
