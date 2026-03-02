from typing import Any, List, Dict
from uuid import UUID
from osgeo import ogr
from .quality_measurement import QualityMeasurement
from .metadata import Metadata
from .result_status import ResultStatus
from .config import DatasetConfig
from ..utils.helpers.geometry import create_buffered_geometry


_QMS_SORT_ORDER = [
    'fullstendighet_dekning',
    'stedfestingsnøyaktighet',
    'egnethet_reguleringsplan',
    'egnethet_kommuneplan',
    'egnethet_byggesak'
]


class Analysis:
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        self.config_id = config_id
        self.config = config
        self.geometry = geometry
        self.run_on_input_geometry: ogr.Geometry | None = None
        self.epsg = epsg
        self.orig_epsg = orig_epsg
        self.geometries: List[ogr.Geometry] = []
        self.guidance_data: Dict[str, Any] | None = None
        self.title: str | None = None
        self.description: str | None = None
        self.guidance_text: str | None = None
        self.guidance_uri: List[Dict[str, Any]] = []
        self.possible_actions: List[str] = []
        self.quality_measurement: List[QualityMeasurement] = []
        self.quality_warning: List[str] = []
        self.buffer = buffer or 0
        self.input_geometry_area: ogr.Geometry | None = None
        self.run_on_input_geometry_json: Dict[str, Any] | None = None
        self.hit_area: float | None = None
        self.distance_to_object: int = 0
        self.raster_result_map: str | None = None
        self.raster_result_image: str | None = None
        self.raster_result_image_bytes: bytes | None = None
        self.cartography: str | None = None
        self.data: List[Dict] = []
        self.themes: List[str] | None = None
        self.run_on_dataset: Metadata | None = None
        self.run_algorithm: List[str] = []
        self.result_status: ResultStatus = ResultStatus.NO_HIT_GREEN
        self.coverage_statuses: List[str] = []
        self.has_coverage: bool = True
        self.is_relevant = True

    def set_input_geometry(self) -> None:
        self.run_algorithm.append('set input_geometry')

        if self.buffer > 0:
            buffered_geom = create_buffered_geometry(
                self.geometry, self.buffer, self.epsg)
            self.run_algorithm.append(f'add buffer ({self.buffer})')
            self.run_on_input_geometry = buffered_geom
        else:
            self.run_on_input_geometry = self.geometry.Clone()

    def calculate_geometry_areas(self) -> None:
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

        self.run_algorithm.append('calculate hit area')
        self.hit_area = round(hit_area, 2)

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
            'data': self.data,
            'themes': self.themes,
            'runOnDataset': self.run_on_dataset.to_dict() if self.run_on_dataset is not None else None,
            'description': self.description,
            'guidanceText': self.guidance_text,
            'guidanceUri': self.guidance_uri,
            'possibleActions': self.possible_actions,
            'qualityMeasurement': list(map(lambda item: item.to_dict(), sorted_qms)),
            'qualityWarning': self.quality_warning
        }

    def __sort_quality_measurements(self) -> List[QualityMeasurement]:
        qms: List[QualityMeasurement] = []

        for id in _QMS_SORT_ORDER:
            result = [
                qm for qm in self.quality_measurement if qm.quality_dimension_id == id]

            if len(result) > 0:
                qms.extend(result)

        return qms
