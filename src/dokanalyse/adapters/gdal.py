from typing import Dict, List, Any
from osgeo import ogr, osr
from ..utils.helpers.geometry import create_feature_collection, transform_geometry


def query_gdal(driver_name: str, data_source: Any, filter: str, geometry: ogr.Geometry, epsg: int) -> Dict:
    driver: ogr.Driver = ogr.GetDriverByName(driver_name)
    ds: ogr.DataSource = driver.Open(data_source)
    layer: ogr.Layer = ds.GetLayer(0)

    sr: osr.SpatialReference = layer.GetSpatialRef()
    auth_code: str = sr.GetAuthorityCode(None)
    target_epsg = int(auth_code)

    if target_epsg != epsg:
        input_geometry = transform_geometry(geometry, epsg, target_epsg)
    else:
        input_geometry = geometry

    layer.SetSpatialFilter(input_geometry)

    if filter:
        layer.SetAttributeFilter(filter)

    ogr_feature: ogr.Feature
    features: List[Dict] = []

    for ogr_feature in layer:
        feature_geom: ogr.Geometry = ogr_feature.GetGeometryRef()
        clone: ogr.Geometry = feature_geom.Clone()
        properties = {}
        field_count: int = ogr_feature.GetFieldCount()

        for i in range(field_count):
            field: ogr.FieldDefn = ogr_feature.GetFieldDefnRef(i)
            name = field.GetName()
            value = ogr_feature.GetField(i)
            properties[name] = value

        feature = {
            'geometry': clone,
            'properties': properties
        }

        features.append(feature)

    response = create_feature_collection(features, target_epsg)

    return response
