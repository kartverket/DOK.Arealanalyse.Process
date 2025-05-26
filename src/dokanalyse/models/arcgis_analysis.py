from sys import maxsize
import json
from uuid import UUID
from typing import List, Dict
from osgeo import ogr
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig
from ..services.guidance_data import get_guidance_data
from ..services.raster_result import get_wms_url, get_cartography_url
from ..utils.helpers.geometry import create_buffered_geometry, geometry_from_json
from ..drivers.arcgis import query_arcgis


class ArcGisAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        super().__init__(config_id, config, geometry, epsg, orig_epsg, buffer)

    async def _run_queries(self, context: str) -> None:
        first_layer = self.config.layers[0]

        guidance_id = first_layer.building_guidance_id if context == 'Byggesak' else first_layer.planning_guidance_id
        guidance_data = await get_guidance_data(guidance_id)

        self._add_run_algorithm(f'query {self.config.arcgis}')             

        for layer in self.config.layers:
            if layer.filter is not None:
                self._add_run_algorithm(f'add filter {layer.filter}')

            status_code, api_response = await query_arcgis(
                self.config.arcgis, layer.arcgis, layer.filter, self.run_on_input_geometry, self.epsg)

            if status_code == 408:
                self.result_status = ResultStatus.TIMEOUT
                self._add_run_algorithm(f'intersects layer {layer.arcgis} (Timeout)')     
                break
            elif status_code != 200:
                self.result_status = ResultStatus.ERROR
                self._add_run_algorithm(f'intersects layer {layer.arcgis} (Error)')
                break

            if api_response:
                response = self.__parse_response(api_response)

                if len(response['properties']) > 0:
                    self._add_run_algorithm(f'intersects layer {layer.arcgis} (True)')

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

                self._add_run_algorithm(f'intersects layer {layer.arcgis} (False)')

        self.guidance_data = guidance_data

    async def _set_distance_to_object(self) -> None:
        buffered_geom = create_buffered_geometry(
            self.geometry, 20000, self.epsg)
        layer = self.config.layers[0]

        _, response = await query_arcgis(self.config.arcgis, layer.arcgis, layer.filter, buffered_geom, self.epsg)

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

    def __parse_response(self, arcgis_response: Dict) -> Dict[str, List]:
        data = {
            'properties': [],
            'geometries': []
        }

        features: List[Dict] = arcgis_response.get('features', [])

        for feature in features:
            data['properties'].append(
                self.__map_properties(feature, self.config.properties))
            data['geometries'].append(
                self.__get_geometry_from_response(feature))

        return data

    def __map_properties(self, feature: Dict, mappings: List[str]) -> Dict:
        props = {}
        feature_props: Dict = feature['properties']

        for mapping in mappings:
            props[mapping] = feature_props.get(mapping)

        return props

    def __get_geometry_from_response(self, feature) -> ogr.Geometry:
        json_str = json.dumps(feature['geometry'])

        return geometry_from_json(json_str)
