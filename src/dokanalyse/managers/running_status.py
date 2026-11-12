from typing import Optional
import structlog
from structlog.stdlib import BoundLogger
from pygeoapi.process.base import BaseProcessor
from pygeoapi.process.manager.base import Subscriber
from pygeoapi.util import JobStatus, RequestedResponse, get_current_datetime

_logger: BoundLogger = structlog.get_logger(__name__)


class RunningStatusMixin:
    """Marks a job as running before the process executes.

    pygeoapi keeps a job as "accepted" for the whole execution and only sets
    "running" while the output is written. Combine with a pygeoapi manager:
    class MyManager(RunningStatusMixin, PostgreSQLManager).
    """

    def _execute_handler_sync(self, p: BaseProcessor, job_id: str,
                              data_dict: dict,
                              requested_outputs: Optional[dict] = None,
                              subscriber: Optional[Subscriber] = None,
                              requested_response: Optional[RequestedResponse] = RequestedResponse.raw.value):
        try:
            self.update_job(job_id, {
                'updated': get_current_datetime(),
                'status': JobStatus.running.value,
                'message': 'Job running',
                'progress': 10
            })
        except Exception as err:
            # A failed status update must not stop the job from running
            _logger.warning('Could not set job status to running', job_id=job_id, error=str(err))

        return super()._execute_handler_sync(
            p, job_id, data_dict, requested_outputs, subscriber, requested_response)


__all__ = ['RunningStatusMixin']
