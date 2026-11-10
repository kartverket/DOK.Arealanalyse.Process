from osgeo import ogr
from dokanalyse.services import municipality

# U-shaped polygon, the centroid lies in the opening of the U
_U_SHAPE = ('POLYGON ((0 0, 300 0, 300 300, 200 300, 200 100, 100 100, '
            '100 300, 0 300, 0 0))')


async def test_point_used_for_lookup_is_inside_the_polygon(monkeypatch):
    geometry = ogr.CreateGeometryFromWkt(_U_SHAPE)
    points = []

    async def fetch_municipality(x, y, epsg):
        points.append((x, y))
        return '0301', 'Oslo'

    monkeypatch.setattr(municipality, '_fetch_municipality', fetch_municipality)

    assert not geometry.Contains(geometry.Centroid())
    assert await municipality.get_municipality(geometry, 25833) == ('0301', 'Oslo')

    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint_2D(*points[0])

    assert geometry.Contains(point)
