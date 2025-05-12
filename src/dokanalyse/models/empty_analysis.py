from uuid import UUID
from .analysis import Analysis
from .result_status import ResultStatus
from .config.dataset_config import DatasetConfig
from ..services.kartkatalog import get_kartkatalog_metadata


class EmptyAnalysis(Analysis):
    def __init__(self, config_id: UUID, config: DatasetConfig, result_status: ResultStatus):
        super().__init__(config_id, config, None, None, None, 0)
        self.result_status = result_status

    async def run(self):
        self.title = self.geolett['tittel'] if self.geolett else self.config.title
        self.themes = self.config.themes
        self.run_on_dataset = await get_kartkatalog_metadata(self.config.metadata_id)

    def _add_run_algorithm(self) -> None:
        raise NotImplementedError

    def _run_queries(self) -> None:
        return NotImplementedError

    def _set_distance_to_object(self) -> None:
        return NotImplementedError
