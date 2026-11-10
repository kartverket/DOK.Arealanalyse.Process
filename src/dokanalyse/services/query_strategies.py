import json
from typing import Any, Dict, List, Literal, Protocol, Tuple, TypeVar
from osgeo import ogr
from pydash import get as pydash_get
from .wfs_response import WfsResponseParser
from ..adapters import get_service_url
from ..adapters.wfs import query_wfs
from ..adapters.arcgis import query_arcgis
from ..adapters.ogc_api import query_ogc_api
from ..models.analysis import Analysis
from ..models.config.dataset_config import DatasetConfig
from ..models.config.layer import Layer
from ..utils.helpers.geometry import geometry_from_json
from ..utils.helpers.common import objectify_properties

T = TypeVar('T')


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
        return get_service_url(_require(config.wfs, 'wfs'))

    def get_layer_name(self, layer: Layer) -> str:
        return _require(layer.wfs, 'layers.wfs')

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_wfs(
            _require(config.wfs, 'wfs'), self.get_layer_name(layer),
            _require(config.geom_field, 'geom_field'), geometry, epsg, config)

    def parse_response(self, response: Any, config: DatasetConfig, layer: Layer) -> Dict[str, list]:
        parser = WfsResponseParser(config, layer)
        return parser.parse(response)

    def extract_geometries(self, response: Any, config: DatasetConfig, layer: Layer) -> List[ogr.Geometry]:
        parsed = self.parse_response(response, config, layer)
        return parsed.get('geometries', [])


class ArcGisQueryStrategy:
    def get_service_url(self, config: DatasetConfig) -> str:
        return get_service_url(_require(config.arcgis, 'arcgis'))

    def get_layer_name(self, layer: Layer) -> str:
        return _require(layer.arcgis, 'layers.arcgis')

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_arcgis(
            _require(config.arcgis, 'arcgis'), self.get_layer_name(layer), layer.filter, geometry, epsg, config)

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
        return get_service_url(_require(config.ogc_api, 'ogc_api'))

    def get_layer_name(self, layer: Layer) -> str:
        return _require(layer.ogc_api, 'layers.ogc_api')

    async def query(self, config: DatasetConfig, layer: Layer, geometry: ogr.Geometry, epsg: int) -> Tuple[int, Any]:
        return await query_ogc_api(
            _require(config.ogc_api, 'ogc_api'), self.get_layer_name(layer),
            _require(config.geom_field, 'geom_field'), geometry, layer.filter, epsg, config)

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


_strategies: Dict[str, QueryStrategy] = {
    'wfs': WfsQueryStrategy(),
    'arcgis': ArcGisQueryStrategy(),
    'ogc_api': OgcApiQueryStrategy(),
}


def get_query_strategy(analysis: Analysis) -> QueryStrategy | None:
    service_type = analysis.config.service_type

    return _strategies.get(service_type) if service_type else None


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


def _geometry_from_feature_json(feature: Dict[str, Any]) -> ogr.Geometry | None:
    json_str = json.dumps(feature['geometry'])
    return geometry_from_json(json_str)


def _require(value: T | None, name: str) -> T:
    if value is None:
        raise ValueError(f'The dataset config is missing "{name}"')

    return value


__all__ = ['QueryStrategy', 'get_query_strategy']
