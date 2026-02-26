from uuid import UUID
from osgeo import ogr
from .analysis import Analysis
from .config.dataset_config import DatasetConfig


class OgcApiAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int):
        super().__init__(config_id, config, geometry, epsg, orig_epsg, buffer)
