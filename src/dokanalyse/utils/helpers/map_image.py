from osgeo import ogr
from .geometry import create_feature, create_feature_collection, create_buffered_geometry
from ..constants import DEFAULT_EPSG
from ...models.map_image_payload import MapImagePayload


def create_payload_for_analysis(geometry: ogr.Geometry, buffer: int, wms_url: str) -> MapImagePayload:
    wmts = {
        'url': 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml',
        'layer': 'topograatone'
    }

    features = [create_feature(geometry)]

    if buffer > 0:
        buffered_geometry = create_buffered_geometry(
            geometry, buffer, DEFAULT_EPSG)
        features.append(create_feature(buffered_geometry, {'buffer': True}))

    feature_collection = create_feature_collection(features, DEFAULT_EPSG)

    styling = [
        {
            'filter': ['has', 'buffer'],
            'style': {
                'stroke-color': '#d33333',
                'stroke-line-dash': [8, 8],
                'stroke-width': 2
            }
        },
        {
            'else': True,
            'style': {
                'stroke-color': '#d33333',
                'stroke-width': 4
            }
        }
    ]

    return MapImagePayload(1280, 720, wmts, [wms_url], feature_collection, styling)


def create_payload_for_fact_sheet(geometry: ogr.Geometry, buffer: int) -> MapImagePayload:
    wmts = {
        'url': 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml',
        'layer': 'topo'
    }

    features = [create_feature(geometry)]

    if buffer > 0:
        buffered_geometry = create_buffered_geometry(
            geometry, buffer, DEFAULT_EPSG)
        features.append(create_feature(buffered_geometry, {'buffer': True}))

    feature_collection = create_feature_collection(features, DEFAULT_EPSG)

    styling = [
        {
            'filter': ['has', 'buffer'],
            'style': {
                'stroke-color': '#d33333',
                'stroke-line-dash': [8, 8],
                'stroke-width': 2
            }
        },
        {
            'else': True,
            'style': {
                'stroke-color': '#d33333',
                'stroke-width': 4
            }
        }
    ]

    return MapImagePayload(1280, 548, wmts, None, feature_collection, styling)


__all__ = ['create_payload_for_analysis', 'create_payload_for_fact_sheet']