import json
from typing import Any, Dict, List, Literal, Protocol, Tuple, cast
from osgeo import ogr
from pydash import get as pydash_get
from .wfs_response import WfsResponseParser
from ..adapters import get_service_url
from ..adapters.wfs import query_wfs
from ..adapters.arcgis import query_arcgis
from ..adapters.ogc_api import query_ogc_api
from ..models.analysis import Analysis
from ..models.wfs_analysis import WfsAnalysis
from ..models.arcgis_analysis import ArcGisAnalysis
from ..models.ogc_api_analysis import OgcApiAnalysis
from ..models.config.dataset_config import DatasetConfig
from ..models.config.layer import Layer
from ..utils.helpers.geometry import geometry_from_json
from ..utils.helpers.common import objectify_properties


class QueryStrategy(Protocol):
    def get_service_url(self, config: DatasetConfig) -> str: ...
    def get_layer_name(self, layer: Layer) -> str: ...

    async def query(self, config: DatasetConfig, layer: Layer,
                    geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]: ...

    def parse_response(self, response: Any, config: DatasetConfig,
                       layer: Layer) -> Dict[str, list]: ...
    def extract_geometries(self, response: Any, config: DatasetConfig,
                           layer: Layer) -> List[ogr.Geometry]: ...


class WfsQueryStrategy:
    def get_service_url(self, config: DatasetConfig) -> str:
        return get_service_url(config.wfs)

    def get_layer_name(self, layer: Layer) -> str:
        return layer.wfs

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_wfs(config.wfs, layer.wfs, config.geom_field, geometry, epsg, config)

    def parse_response(self, response: Any, config: DatasetConfig, layer: Layer) -> Dict[str, list]:
        parser = WfsResponseParser(config, layer)
        return parser.parse(response)

    def extract_geometries(self, response: Any, config: DatasetConfig, layer: Layer) -> List[ogr.Geometry]:
        parsed = self.parse_response(response, config, layer)
        return parsed.get('geometries', [])


class ArcGisQueryStrategy:
    def get_service_url(self, config: DatasetConfig) -> str:
        return get_service_url(config.arcgis)

    def get_layer_name(self, layer: Layer) -> str:
        return layer.arcgis

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_arcgis(config.arcgis, layer.arcgis, layer.filter, geometry, epsg, config)

    def parse_response(self, response: Any, config: DatasetConfig, layer: Layer) -> Dict[str, list]:
        data = {
            'properties': [],
            'geometries': []
        }

        features: List[Dict] = response.get('features', [])

        for feature in features:
            data['properties'].append(
                _map_properties(feature, config.properties, config.skip_properties))
            data['geometries'].append(
                _geometry_from_feature_json(feature))

        return data

    def extract_geometries(self, response: Any, config: DatasetConfig, layer: Layer) -> List[ogr.Geometry]:
        geometries = []

        for feature in response.get('features', []):
            geom = _geometry_from_feature_json(feature)
            if geom is not None:
                geometries.append(geom)

        return geometries


class OgcApiQueryStrategy:
    def get_service_url(self, config: DatasetConfig) -> str:
        return get_service_url(config.ogc_api)

    def get_layer_name(self, layer: Layer) -> str:
        return layer.ogc_api

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_ogc_api(config.ogc_api, layer.ogc_api, config.geom_field, geometry, layer.filter, epsg, config)

    def parse_response(self, response: List[Dict[str, Any]], config: DatasetConfig, layer: Layer) -> Dict[str, list]:
        data = {
            'properties': [],
            'geometries': []
        }

        feature: Dict[str, Any]

        for feature in response:
            data['properties'].append(
                _map_properties(feature, config.properties, config.skip_properties))
            data['geometries'].append(feature['geometry'])

        return data

    def extract_geometries(self, response: List[Dict[str, Any]], config: DatasetConfig, layer: Layer) -> List[ogr.Geometry]:
        geometries: List[ogr.Geometry] = []

        for feature in response:
            geom: ogr.Geometry = feature['geometry']

            if geom:
                geometries.append(geom)

        return geometries


_strategies: Dict[type, QueryStrategy] = {
    WfsAnalysis: WfsQueryStrategy(),
    ArcGisAnalysis: ArcGisQueryStrategy(),
    OgcApiAnalysis: OgcApiQueryStrategy(),
}


def get_query_strategy(analysis: Analysis) -> QueryStrategy | None:
    return _strategies.get(type(analysis))


def _map_properties(
    feature: Dict[str, Any],
    properties: List[str] | Literal['*'],
    skip_properties: List[str]
) -> Dict[str, Any]:
    if isinstance(properties, str):
        props = feature['properties']

        for prop_name in skip_properties:
            if prop_name in props:
                del props[prop_name]

        return props

    props = {}

    for prop_name in properties:
        if prop_name in skip_properties:
            continue

        value = pydash_get(feature['properties'], prop_name, None)
        props[prop_name] = value

    return objectify_properties(props)


def _geometry_from_feature_json(feature: Dict[str, Any]) -> ogr.Geometry:
    json_str = json.dumps(feature['geometry'])
    return geometry_from_json(json_str)


__all__ = ['QueryStrategy', 'get_query_strategy']
