import json
from sys import maxsize
from typing import List, Dict
from uuid import UUID
from pydash import get
from osgeo import ogr
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig
from ..services.guidance_data import get_guidance_data
from ..services.raster_result import get_wms_url, get_cartography_url
from ..utils.helpers.geometry import create_buffered_geometry, geometry_from_json, transform_geometry
from ..drivers.ogc_api import query_ogc_api


class OgcApiAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        super().__init__(config_id, config, geometry, epsg, orig_epsg, buffer)

    async def _run_queries(self, context: str) -> None:
        first_layer = self.config.layers[0]

        guidance_id = first_layer.building_guidance_id if context == 'Byggesak' else first_layer.planning_guidance_id
        guidance_data = await get_guidance_data(guidance_id)

        self._add_run_algorithm(f'query {self.config.ogc_api}')

        for layer in self.config.layers:
            if layer.filter is not None:
                self._add_run_algorithm(f'add filter {layer.filter}')

            status_code, api_response = await query_ogc_api(
                self.config.ogc_api, layer.ogc_api, self.config.geom_field, self.run_on_input_geometry, layer.filter, self.epsg)

            if status_code == 408:
                self.result_status = ResultStatus.TIMEOUT
                self._add_run_algorithm(
                    f'intersects layer {layer.ogc_api} (Timeout)')
                break
            elif status_code != 200:
                self.result_status = ResultStatus.ERROR
                self._add_run_algorithm(
                    f'intersects layer {layer.ogc_api} (Error)')
                break

            if api_response:
                response = self.__parse_response(api_response)

                if len(response['properties']) > 0:
                    self._add_run_algorithm(
                        f'intersects layer {layer.ogc_api} (True)')

                    guidance_id = layer.building_guidance_id if context == 'Byggesak' else layer.planning_guidance_id
                    guidance_data = await get_guidance_data(guidance_id)

                    self.data = response['properties']
                    self.geometries = response['geometries']
                    self.raster_result_map = get_wms_url(
                        self.config.wms, layer.wms)
                    self.cartography = await get_cartography_url(
                        self.config.wms, layer.wms)
                    self.result_status = layer.result_status
                    break

                self._add_run_algorithm(
                    f'intersects layer {layer.ogc_api} (False)')

        self.guidance_data = guidance_data

    async def _set_distance_to_object(self) -> None:
        buffered_geom = create_buffered_geometry(
            self.geometry, 20000, self.epsg)
        layer = self.config.layers[0]

        _, response = await query_ogc_api(self.config.ogc_api, layer.ogc_api, self.config.geom_field, buffered_geom, layer.filter, self.epsg)

        if response is None:
            self.distance_to_object = maxsize
            return

        distances = []

        for feature in response['features']:
            feature_geom = self.__get_geometry_from_response(feature)

            if feature_geom is not None:
                distance = round(
                    self.run_on_input_geometry.Distance(feature_geom))
                distances.append(distance)

        distances.sort()
        self._add_run_algorithm('get distance to nearest object')

        if len(distances) == 0:
            self.distance_to_object = maxsize
        else:
            self.distance_to_object = distances[0]

    def __parse_response(self, ogc_api_response: Dict) -> Dict[str, List]:
        data = {
            'properties': [],
            'geometries': []
        }

        for feature in ogc_api_response['features']:
            data['properties'].append(self.__map_properties(
                feature, self.config.properties))
            data['geometries'].append(
                self.__get_geometry_from_response(feature))

        return data

    def __map_properties(self, feature: Dict, mappings: List[str]) -> Dict:
        props = {}

        for mapping in mappings:
            key = mapping.split('.')[-1]
            value = get(feature['properties'], mapping, None)
            props[key] = value

        return props

    def __get_geometry_from_response(self, feature: Dict) -> ogr.Geometry:
        json_str = json.dumps(feature['geometry'])
        geometry = geometry_from_json(json_str)

        return transform_geometry(geometry, 4326, 25833)
