import json
from osgeo import ogr
from dokanalyse.utils.helpers.geometry import (
    create_input_geometry, create_buffered_geometry, geometry_to_arcgis_geom,
    envelope_to_polygon, get_epsg_from_geojson, get_epsg_from_crs, add_geojson_crs,
    geometry_to_wkt, length_to_degrees)


def test_create_input_geometry_keeps_25833(polygon_25833):
    geometry = create_input_geometry(polygon_25833)

    assert geometry.GetArea() == 10_000
    assert geometry.GetEnvelope() == (262400.0, 262500.0, 6649000.0, 6649100.0)


def test_create_input_geometry_transforms_wgs84_lon_lat(polygon_wgs84):
    geometry = create_input_geometry(polygon_wgs84)
    minx, maxx, miny, maxy = geometry.GetEnvelope()

    # Oslo in UTM 33N
    assert 262_000 < minx < maxx < 264_000
    assert 6_648_000 < miny < maxy < 6_651_000


def test_create_buffered_geometry_in_meters(geometry_25833):
    buffered = create_buffered_geometry(geometry_25833, 10, 25833)
    minx, maxx, miny, maxy = buffered.GetEnvelope()

    assert round(minx) == 262390 and round(maxx) == 262510
    assert round(miny) == 6648990 and round(maxy) == 6649110


def test_length_to_degrees():
    assert round(length_to_degrees(111_195), 3) == 1.0


def test_geometry_to_arcgis_geom(geometry_25833):
    arcgis_geom = json.loads(geometry_to_arcgis_geom(geometry_25833, 25833))

    assert arcgis_geom['spatialReference'] == {'wkid': 25833}
    assert arcgis_geom['rings'][0][0] == [262400.0, 6649000.0]


def test_envelope_to_polygon():
    polygon = envelope_to_polygon((0.0, 10.0, 0.0, 5.0))

    assert polygon.GetGeometryType() == ogr.wkbPolygon
    assert polygon.GetArea() == 50


def test_geometry_to_wkt_rounds_coordinates():
    geometry = ogr.CreateGeometryFromWkt('POINT (1.23456 2.34567)')

    assert geometry_to_wkt(geometry, 25833) == 'POINT (1.23 2.35)'


def test_get_epsg_from_geojson(polygon_25833, polygon_wgs84):
    assert get_epsg_from_geojson(polygon_25833) == 25833
    assert get_epsg_from_geojson(polygon_wgs84) is None

    for crs in ['EPSG:4258', 'http://www.opengis.net/def/crs/EPSG/0/4258', 'urn:ogc:def:crs:EPSG::4258']:
        assert get_epsg_from_geojson({'crs': {'properties': {'name': crs}}}) == 4258


def test_get_epsg_from_crs():
    assert get_epsg_from_crs('urn:ogc:def:crs:EPSG::25833') == 25833
    assert get_epsg_from_crs('EPSG:4326') == 4326


def test_add_geojson_crs():
    geojson = {}
    add_geojson_crs(geojson, 25832)
    assert geojson['crs']['properties']['name'] == 'urn:ogc:def:crs:EPSG::25832'

    geojson = {}
    add_geojson_crs(geojson, None)
    assert 'crs' not in geojson


def test_input_with_geographic_crs_is_read_as_lon_lat(polygon_wgs84):
    from_wgs84 = create_input_geometry(polygon_wgs84)
    polygon_4258 = dict(polygon_wgs84, crs={'type': 'name', 'properties': {'name': 'urn:ogc:def:crs:EPSG::4258'}})
    from_4258 = create_input_geometry(polygon_4258)

    for a, b in zip(from_wgs84.GetEnvelope(), from_4258.GetEnvelope(), strict=True):
        assert abs(a - b) < 1


def test_run_on_input_geometry_json_keeps_input_crs(polygon_wgs84, polygon_25833):
    from dokanalyse.utils.helpers.geometry import create_run_on_input_geometry_json

    geometry = create_input_geometry(polygon_wgs84)
    result = create_run_on_input_geometry_json(geometry, 25833, None)
    lon, lat = result['coordinates'][0][0]
    assert 'crs' not in result
    assert abs(lon - 10.75) < 1e-6 and abs(lat - 59.91) < 1e-6

    polygon_4258 = dict(polygon_wgs84, crs={'type': 'name', 'properties': {'name': 'EPSG:4258'}})
    result = create_run_on_input_geometry_json(create_input_geometry(polygon_4258), 25833, 4258)
    lon, lat = result['coordinates'][0][0]
    assert result['crs']['properties']['name'] == 'urn:ogc:def:crs:EPSG::4258'
    assert abs(lon - 10.75) < 1e-6 and abs(lat - 59.91) < 1e-6

    result = create_run_on_input_geometry_json(create_input_geometry(polygon_25833), 25833, 25833)
    assert result['crs']['properties']['name'] == 'urn:ogc:def:crs:EPSG::25833'
    assert result['coordinates'][0][0] == [262400.0, 6649000.0]


def test_self_intersecting_polygon_is_repaired():
    import pytest
    bowtie = {'type': 'Polygon', 'coordinates': [[[0, 0], [10, 10], [10, 0], [0, 10], [0, 0]]],
              'crs': {'type': 'name', 'properties': {'name': 'EPSG:25833'}}}

    geometry = create_input_geometry(bowtie)

    assert geometry.IsValid()
    assert geometry.GetArea() == pytest.approx(50)


def test_empty_polygon_is_rejected():
    import pytest
    empty = {'type': 'Polygon', 'coordinates': [],
             'crs': {'type': 'name', 'properties': {'name': 'EPSG:25833'}}}

    with pytest.raises(ValueError):
        create_input_geometry(empty)
