from sys import maxsize
from io import BytesIO
from typing import List, Dict
from osgeo import ogr
from lxml import etree as ET
from uuid import UUID
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig
from .config.layer import Layer
from ..services.guidance_data import get_guidance_data
from ..services.raster_result import get_wms_url, get_cartography_url
from ..utils.helpers.common import parse_string, evaluate_condition, xpath_select_one
from ..utils.helpers.geometry import create_buffered_geometry, geometry_from_gml
from ..drivers.wfs import query_wfs


class WfsAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        super().__init__(config_id, config, geometry, epsg, orig_epsg, buffer)

    async def _run_queries(self, context: str) -> None:
        first_layer = self.config.layers[0]

        guidance_id = first_layer.building_guidance_id if context == 'Byggesak' else first_layer.planning_guidance_id
        guidance_data = await get_guidance_data(guidance_id)

        self._add_run_algorithm(f'query {self.config.wfs}')

        for layer in self.config.layers:
            if layer.filter is not None:
                self._add_run_algorithm(f'add filter {layer.filter}')

            status_code, api_response = await query_wfs(
                self.config.wfs, layer.wfs, self.config.geom_field, self.run_on_input_geometry, self.epsg)

            if status_code == 408:
                self.result_status = ResultStatus.TIMEOUT
                self._add_run_algorithm(
                    f'intersects layer {layer.wfs} (Timeout)')
                break
            elif status_code != 200:
                self.result_status = ResultStatus.ERROR
                self._add_run_algorithm(
                    f'intersects layer {layer.wfs} (Error)')
                break

            if api_response:
                response = self.__parse_response(api_response, layer)

                if len(response['properties']) > 0:
                    self._add_run_algorithm(
                        f'intersects layer {layer.wfs} (True)')

                    guidance_id = layer.building_guidance_id if context == 'Byggesak' else layer.planning_guidance_id
                    guidance_data = await get_guidance_data(guidance_id)

                    self.geometries = response['geometries']
                    self.data = response['properties']
                    self.raster_result_map = get_wms_url(
                        self.config.wms, layer.wms)
                    self.cartography = await get_cartography_url(
                        self.config.wms, layer.wms)
                    self.result_status = layer.result_status
                    break

                self._add_run_algorithm(
                    f'intersects layer {layer.wfs} (False)')

        self.guidance_data = guidance_data

    async def _set_distance_to_object(self) -> None:
        buffered_geom = create_buffered_geometry(
            self.geometry, 20000, self.epsg)
        layer = self.config.layers[0]

        _, api_response = await query_wfs(self.config.wfs, layer.wfs, self.config.geom_field, buffered_geom, self.epsg)

        if api_response is None:
            self.distance_to_object = maxsize
            return

        response = self.__parse_response(api_response, layer)
        geometries: List[ogr.Geometry] = response.get('geometries')
        distances = []

        for geom in geometries:
            distance = round(self.run_on_input_geometry.Distance(geom))
            distances.append(distance)

        distances.sort()
        self._add_run_algorithm('get distance to nearest object')

        if len(distances) == 0:
            self.distance_to_object = maxsize
        else:
            self.distance_to_object = distances[0]

    def __parse_response(self, wfs_response: str, layer: Dict) -> Dict[str, List]:
        data = {
            'properties': [],
            'geometries': []
        }

        source = BytesIO(wfs_response.encode('utf-8'))
        context = ET.iterparse(source, huge_tree=True)

        for _, elem in context:
            localname = ET.QName(elem).localname

            if localname != 'member':
                continue

            props = self.__map_properties(elem)

            if self.__filter_member(props, layer):
                data['properties'].append(props)
                data['geometries'].append(
                    self.__get_geometry_from_response(elem))

        return data

    def __filter_member(self, props: Dict, layer: Layer) -> bool:
        if not layer.filter:
            return True

        return evaluate_condition(layer.filter, props)

    def __map_properties(self, member: ET._Element) -> Dict:
        props = {}

        for mapping in self.config.properties:
            path = f'.//*[local-name() = "{mapping}"]/text()'
            value = xpath_select_one(member, path)

            if value:
                prop_name = mapping
                props[prop_name] = parse_string(value)

        return props

    def __get_geometry_from_response(self, member: ET._Element) -> ogr.Geometry:
        geom_field = self.config.geom_field
        path = f'.//*[local-name() = "{geom_field}"]/*'
        geom_elem = xpath_select_one(member, path)

        if geom_elem is None:
            return None

        gml_str = ET.tostring(geom_elem, encoding='unicode')

        return geometry_from_gml(gml_str)
