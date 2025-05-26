from abc import ABC, abstractmethod
from typing import List, Dict
from uuid import UUID
from osgeo import ogr
from .quality_measurement import QualityMeasurement
from .metadata import Metadata
from .result_status import ResultStatus
from .config import DatasetConfig
from ..utils.helpers.common import keys_to_camel_case
from ..utils.helpers.geometry import create_buffered_geometry, create_run_on_input_geometry_json
from ..utils.helpers.map_image import create_payload_for_analysis
from ..utils.helpers.quality import get_coverage_indicator, get_coverage_service_config_data
from ..utils.constants import MAP_IMAGE_API_URL
from ..services.config import get_quality_indicator_configs
from ..services.kartkatalog import get_kartkatalog_metadata
from ..services.quality.coverage_quality import get_coverage_quality
from ..services.quality.dataset_quality import get_dataset_quality
from ..services.quality.object_quality import get_object_quality
from ..services.guidance_data import get_guidance_data
from ..services.map_image import create_map_image

_QMS_SORT_ORDER = [
    'fullstendighet_dekning',
    'stedfestingsnøyaktighet',
    'egnethet_reguleringsplan',
    'egnethet_kommuneplan',
    'egnethet_byggesak'
]


class Analysis(ABC):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        self.config_id = config_id
        self.config = config
        self.geometry = geometry
        self.run_on_input_geometry: ogr.Geometry = None
        self.epsg = epsg
        self.orig_epsg = orig_epsg
        self.geometries: List[ogr.Geometry] = []
        self.guidance_data: Dict = None
        self.title: str = None
        self.description: str = None
        self.guidance_text: str = None
        self.guidance_uri: List[Dict] = []
        self.possible_actions: List[str] = []
        self.quality_measurement: List[QualityMeasurement] = []
        self.quality_warning: List[str] = []
        self.buffer = buffer or 0
        self.input_geometry_area: ogr.Geometry = None
        self.run_on_input_geometry_json: Dict = None
        self.hit_area: float = None
        self.distance_to_object: int = 0
        self.raster_result_map: str = None
        self.raster_result_image: str = None
        self.raster_result_image_bytes: bytes = None
        self.cartography: str = None
        self.data: List[Dict] = []
        self.themes: List[str] = None
        self.run_on_dataset: Metadata = None
        self.run_algorithm: List[str] = []
        self.result_status: ResultStatus = ResultStatus.NO_HIT_GREEN
        self.coverage_statuses: List[str] = []
        self.has_coverage: bool = True

    async def run(self, context, include_guidance, include_quality_measurement) -> None:
        self.__set_input_geometry()

        await self.__run_coverage_analysis(context)

        if self.has_coverage:
            await self._run_queries(context)

            if self.result_status == ResultStatus.TIMEOUT or self.result_status == ResultStatus.ERROR:
                await self.set_default_data()
                return

            self.__set_geometry_areas()
        else:
            self.result_status = ResultStatus.NO_HIT_YELLOW

        if self.result_status in [ResultStatus.NO_HIT_GREEN, ResultStatus.NO_HIT_YELLOW]:
            await self._set_distance_to_object()

        self._add_run_algorithm('deliver result')

        self.run_on_input_geometry_json = create_run_on_input_geometry_json(
            self.run_on_input_geometry, self.epsg, self.orig_epsg)

        await self.set_default_data()

        if include_guidance and self.guidance_data is not None:
            self.__set_guidance_data()

        if include_quality_measurement:
            await self.__set_quality_measurements(context)

        if self.raster_result_map and MAP_IMAGE_API_URL:
            payload = create_payload_for_analysis(
                self.geometry, self.buffer, self.raster_result_map)
            _, result = await create_map_image(payload)
            self.raster_result_image_bytes = result

    def _add_run_algorithm(self, algorithm) -> None:
        self.run_algorithm.append(algorithm)

    async def set_default_data(self) -> None:
        self.title = self.guidance_data.get(
            'tittel') if self.guidance_data else self.config.title
        self.themes = self.config.themes
        self.run_on_dataset = await get_kartkatalog_metadata(self.config.metadata_id)

    async def __run_coverage_analysis(self, context) -> None:
        quality_indicators = get_quality_indicator_configs(self.config_id)
        ci = get_coverage_indicator(quality_indicators)
        
        if not ci:
            return

        coverage_svc = get_coverage_service_config_data(ci)

        self._add_run_algorithm(f'check coverage {coverage_svc.get("url")}')
        measurements, warning, has_coverage, data = await get_coverage_quality(ci, self.run_on_input_geometry, self.epsg)
        self._add_run_algorithm(
            f'intersects layer {coverage_svc.get("layer")} ({has_coverage})')

        if not has_coverage:
            self.data = data
            guidance_id = coverage_svc.get(
                'building_guidance_id') if context == 'Byggesak' else coverage_svc.get('planning_guidance_id')

            if guidance_id:
                self.guidance_data = await get_guidance_data(guidance_id)

        self.quality_measurement.extend(measurements)
        self.has_coverage = has_coverage

        if warning is not None:
            self.quality_warning.append(warning)

    def __set_input_geometry(self) -> None:
        self._add_run_algorithm('set input_geometry')

        if self.buffer > 0:
            buffered_geom = create_buffered_geometry(
                self.geometry, self.buffer, self.epsg)
            self._add_run_algorithm(f'add buffer ({self.buffer})')
            self.run_on_input_geometry = buffered_geom
        else:
            self.run_on_input_geometry = self.geometry.Clone()

    def __set_geometry_areas(self) -> None:
        self.input_geometry_area = round(
            self.run_on_input_geometry.GetArea(), 2)

        if len(self.geometries) == 0:
            return

        hit_area: float = 0

        for geometry in self.geometries:
            if geometry is None:
                continue

            intersection: ogr.Geometry = self.run_on_input_geometry.Intersection(
                geometry)

            if intersection is None:
                continue

            geom_type = intersection.GetGeometryType()

            if geom_type == ogr.wkbPolygon or geom_type == ogr.wkbMultiPolygon:
                hit_area += intersection.GetArea()

        self._add_run_algorithm('calculate hit area')
        self.hit_area = round(hit_area, 2)

    def __set_guidance_data(self) -> None:
        if self.result_status != ResultStatus.NO_HIT_GREEN:
            self.description = self.guidance_data.get('forklarendeTekst')
            self.guidance_text = self.guidance_data.get('dialogtekst')

        for link in self.guidance_data.get('lenker', []):
            self.guidance_uri.append({
                'href': link['href'],
                'title': link['tittel']
            })

        possible_actions: str = self.guidance_data.get('muligeTiltak', '')

        for line in possible_actions.splitlines():
            self.possible_actions.append(line.lstrip('- '))

    async def __set_quality_measurements(self, context) -> None:
        quality_indicators = get_quality_indicator_configs(self.config_id)

        if len(quality_indicators) == 0:
            return

        dataset_qms, dataset_warnings = await get_dataset_quality(self.config, quality_indicators, context=context, themes=self.themes)
        object_qms, object_warnings = [], []

        if self.has_coverage:
            object_qms, object_warnings = get_object_quality(
                quality_indicators, self.data)

        self.quality_measurement.extend(dataset_qms)
        self.quality_measurement.extend(object_qms)
        self.quality_warning.extend(dataset_warnings)
        self.quality_warning.extend(object_warnings)

    def __sort_quality_measurements(self) -> List[QualityMeasurement]:
        qms: List[QualityMeasurement] = []

        for id in _QMS_SORT_ORDER:
            result = [
                qm for qm in self.quality_measurement if qm.quality_dimension_id == id]

            if len(result) > 0:
                qms.extend(result)

        return qms

    def to_dict(self) -> Dict:
        sorted_qms = self.__sort_quality_measurements()

        return {
            'title': self.title,
            'runOnInputGeometry': self.run_on_input_geometry_json,
            'buffer': self.buffer,
            'runAlgorithm': self.run_algorithm,
            'inputGeometryArea': self.input_geometry_area,
            'hitArea': self.hit_area,
            'resultStatus': self.result_status,
            'distanceToObject': self.distance_to_object,
            'rasterResult': {
                'imageUri': self.raster_result_image,
                'mapUri': self.raster_result_map
            },
            'cartography': self.cartography,
            'data': list(map(lambda entry: keys_to_camel_case(entry), self.data)),
            'themes': self.themes,
            'runOnDataset': self.run_on_dataset.to_dict() if self.run_on_dataset is not None else None,
            'description': self.description,
            'guidanceText': self.guidance_text,
            'guidanceUri': self.guidance_uri,
            'possibleActions': self.possible_actions,
            'qualityMeasurement': list(map(lambda item: item.to_dict(), sorted_qms)),
            'qualityWarning': self.quality_warning
        }

    @abstractmethod
    async def _run_queries(self, context: str) -> None:
        pass

    @abstractmethod
    def _set_distance_to_object(self) -> None:
        pass
