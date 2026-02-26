from uuid import UUID
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig


class EmptyAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, result_status: ResultStatus):
        super().__init__(config_id, config, None, None, None, 0)
        self.result_status = result_status
