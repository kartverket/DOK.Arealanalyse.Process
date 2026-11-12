import atexit
import threading
from typing import Any, Dict, Tuple
from osgeo import ogr, osr, gdal
from pydantic import ValidationError
import structlog
from structlog.contextvars import bind_contextvars
from structlog.stdlib import BoundLogger
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .models import AnalysisInput
from .services import analyses
from .utils.helpers.request import get_validation_error_message
from .utils.http_context import close_session
from .utils.socket_io import get_client
from .utils.logger import setup as setup_logger
from .utils.correlation import set_correlation_id, clear_correlation_id
from .utils.async_executor import exec_async, is_running as executor_is_running
from .constants import MAX_CONCURRENT_ANALYSES
from .metadata import PROCESS_METADATA

gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

setup_logger()

_logger: BoundLogger = structlog.get_logger(__name__)

# Limits concurrent analyses (sync requests and async jobs) in this process.
# pygeoapi starts a new thread per async job, so without a limit a burst of
# jobs would all run at once on the shared event loop and render thread.
_analysis_slots = threading.BoundedSemaphore(MAX_CONCURRENT_ANALYSES)


class DokanalyseProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self._job_id: str | None = None

    def set_job_id(self, job_id: str) -> None:
        # Called by the pygeoapi process manager before execute()
        self._job_id = job_id

    def execute(self, data: Dict[str, Any], outputs=None) -> Tuple[str, Dict[str, Any] | None]:
        try:
            analysis_input = AnalysisInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(f'Invalid payload: {get_validation_error_message(err)}') from err

        set_correlation_id(analysis_input.correlation_id)

        if self._job_id:
            bind_contextvars(job_id=self._job_id)

        if not _analysis_slots.acquire(blocking=False):
            _logger.info('Max concurrent analyses reached, waiting for a free slot',
                         max_concurrent_analyses=MAX_CONCURRENT_ANALYSES)
            _analysis_slots.acquire()

        try:
            outputs = exec_async(self._run_analyses(analysis_input))
        finally:
            _analysis_slots.release()
            clear_correlation_id()

        return 'application/json', outputs

    def __repr__(self) -> str:
        return f'<DokanalyseProcessor> {self.name}'

    async def _run_analyses(self, analysis_input: AnalysisInput) -> Dict[str, Any]:
        sio_client = get_client()

        try:
            return await analyses.run(analysis_input, sio_client)
        finally:
            if sio_client:
                sio_client.disconnect()


@atexit.register
def _close_http_session() -> None:
    # Best effort: the shared HTTP session lives as long as the process
    if executor_is_running():
        try:
            exec_async(close_session(), timeout=5)
        except Exception:
            pass
