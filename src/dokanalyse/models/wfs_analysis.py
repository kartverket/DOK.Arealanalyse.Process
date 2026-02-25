from typing import List
from osgeo import ogr
from uuid import UUID
from .analysis import Analysis
from .result_status import ResultStatus
from ..services.wfs_response import WfsResponseParser
from .config.dataset_config import DatasetConfig
from ..services.guidance_data import get_guidance_data
from ..services.raster_result import get_wms_url, get_cartography_url
from ..utils.helpers.geometry import create_buffered_geometry
from ..adapters import get_service_url
from ..adapters.wfs import query_wfs


class WfsAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        super().__init__(config_id, config, geometry, epsg, orig_epsg, buffer)

    async def _run_queries(self, context: str) -> None:
        first_layer = self.config.layers[0]

        guidance_id = first_layer.building_guidance_id if context.lower(
        ) == 'byggesak' else first_layer.planning_guidance_id
        guidance_data = await get_guidance_data(guidance_id)

        self._add_run_algorithm(f'query {get_service_url(self.config.wfs)}')

        for layer in self.config.layers:
            if layer.filter is not None:
                self._add_run_algorithm(f'add filter {layer.filter}')

            status_code, api_response = await query_wfs(
                self.config.wfs, layer.wfs, self.config.geom_field, self.run_on_input_geometry, self.epsg, self.config)

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
                parser = WfsResponseParser(self.config, layer)
                response = parser.parse(api_response)

                if len(response['properties']) > 0:
                    self._add_run_algorithm(
                        f'intersects layer {layer.wfs} (True)')

                    guidance_id = layer.building_guidance_id if context.lower(
                    ) == 'byggesak' else layer.planning_guidance_id
                    guidance_data = await get_guidance_data(guidance_id)

                    self.geometries = response['geometries']
                    self.data = response['properties']
                    self.raster_result_map = get_wms_url(
                        self.config.wms, layer.wms)
                    self.cartography = await get_cartography_url(
                        self.config.wms, layer.wms, self.run_on_input_geometry)
                    self.result_status = layer.result_status
                    break

                self._add_run_algorithm(
                    f'intersects layer {layer.wfs} (False)')

        self.guidance_data = guidance_data

    async def _set_distance_to_object(self) -> None:
        buffered_geom = create_buffered_geometry(
            self.geometry, 20000, self.epsg)
        layer = self.config.layers[0]

        _, api_response = await query_wfs(self.config.wfs, layer.wfs, self.config.geom_field, buffered_geom, self.epsg, self.config)

        if api_response is None:
            self.distance_to_object = -1
            return

        parser = WfsResponseParser(self.config, layer)
        response = parser.parse(api_response)
        geometries: List[ogr.Geometry] = response.get('geometries')
        distances = []

        for geom in geometries:
            distance = round(self.run_on_input_geometry.Distance(geom))
            distances.append(distance)

        distances.sort()
        self._add_run_algorithm('get distance to nearest object')

        if len(distances) == 0:
            self.distance_to_object = -1
        else:
            self.distance_to_object = distances[0]
