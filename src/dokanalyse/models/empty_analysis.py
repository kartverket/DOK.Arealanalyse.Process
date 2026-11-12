from uuid import UUID
from osgeo import ogr
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig
from ..constants import DEFAULT_EPSG


class EmptyAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, result_status: ResultStatus):
        # Never analyzed, so it has no input geometry
        super().__init__(config_id, config, ogr.Geometry(ogr.wkbPolygon), DEFAULT_EPSG, None, 0)
        self.result_status = result_status
