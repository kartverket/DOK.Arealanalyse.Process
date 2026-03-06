from typing import Any, Dict, Tuple
import asyncio
import aiohttp
from osgeo import ogr, osr, gdal
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .services import analyses
from .caching.xsd import cache_base_xml_schemas
from .utils.helpers.request import request_is_valid
from .utils.http_context import set_session, reset_session
from .utils.socket_io import get_client
from .utils.logger import setup as setup_logger
from .utils.correlation import set_correlation_id, clear_correlation_id
from .utils.async_executor import exec_async
from .metadata import PROCESS_METADATA

gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

setup_logger()
cache_base_xml_schemas()


class DokanalyseProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: Dict[str, Any], outputs=None) -> Tuple[str, Dict[str, Any] | None]:
        set_correlation_id(data.get('correlationId'))

        if not request_is_valid(data):
            raise ProcessorExecuteError('Invalid payload')

        outputs = exec_async(self._run_analyses(data))

        return 'application/json', outputs

    async def _run_analyses(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sio_client = get_client()

        timeout = aiohttp.ClientTimeout(total=30)

        connector = aiohttp.TCPConnector(
            limit=100, limit_per_host=10, ttl_dns_cache=300)

        async with asyncio.Semaphore(50):
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                token = set_session(session)

                try:
                    return await analyses.run(data, sio_client)
                finally:
                    reset_session(token)
                    clear_correlation_id()

                    if sio_client:
                        sio_client.disconnect()

    def __repr__(self) -> str:
        return f'<DokanalyseProcessor> {self.name}'
