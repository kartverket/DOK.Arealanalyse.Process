import atexit
from typing import Any, Dict, Tuple
from osgeo import ogr, osr, gdal
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .services import analyses
from .utils.helpers.request import get_request_validation_error
from .utils.http_context import close_session
from .utils.socket_io import get_client
from .utils.logger import setup as setup_logger
from .utils.correlation import set_correlation_id, clear_correlation_id
from .utils.async_executor import exec_async, is_running as executor_is_running
from .metadata import PROCESS_METADATA

gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

setup_logger()


class DokanalyseProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: Dict[str, Any], outputs=None) -> Tuple[str, Dict[str, Any] | None]:
        validation_error = get_request_validation_error(data)

        if validation_error:
            raise ProcessorExecuteError(f'Invalid payload: {validation_error}')

        set_correlation_id(data.get('correlationId'))

        try:
            outputs = exec_async(self._run_analyses(data))
        finally:
            clear_correlation_id()

        return 'application/json', outputs

    def __repr__(self) -> str:
        return f'<DokanalyseProcessor> {self.name}'

    async def _run_analyses(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sio_client = get_client()

        try:
            return await analyses.run(data, sio_client)
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
