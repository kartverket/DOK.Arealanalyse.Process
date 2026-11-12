import threading
import time
from pygeoapi.process.manager.tinydb_ import TinyDBManager
from pygeoapi.util import JobStatus, RequestedProcessExecutionMode
from dokanalyse.managers.running_status import RunningStatusMixin
from dokanalyse.services import analyses
from conftest import POLYGON_25833


class RunningStatusTinyDBManager(RunningStatusMixin, TinyDBManager):
    pass


def _wait_for_status(manager, job_id, status):
    deadline = time.monotonic() + 10

    while manager.get_job(job_id)['status'] != status:
        assert time.monotonic() < deadline, f'job never reached status {status!r}'
        time.sleep(0.05)


def test_job_is_running_while_process_executes(tmp_path, monkeypatch):
    started = threading.Event()
    release = threading.Event()

    async def fake_run(analysis_input, sio_client):
        started.set()
        release.wait(10)
        return {'resultList': []}

    monkeypatch.setattr(analyses, 'run', fake_run)
    monkeypatch.setattr('dokanalyse.processor.get_client', lambda: None)

    manager = RunningStatusTinyDBManager({
        'name': 'TinyDB',
        'connection': str(tmp_path / 'jobs.db'),
        'output_dir': str(tmp_path),
        'processes': {
            'dokanalyse': {'processor': {'name': 'dokanalyse.processor.DokanalyseProcessor'}}
        }
    })

    job_id, _, _, status, _ = manager.execute_process(
        'dokanalyse', {'inputGeometry': POLYGON_25833},
        execution_mode=RequestedProcessExecutionMode.respond_async)

    assert status == JobStatus.accepted

    try:
        assert started.wait(10), 'process never started'
        assert manager.get_job(job_id)['status'] == 'running'
    finally:
        release.set()

    _wait_for_status(manager, job_id, 'successful')
    assert manager.get_job_result(job_id) == ('application/json', {'resultList': []})


def test_job_runs_when_running_status_update_fails(tmp_path, monkeypatch):
    async def fake_run(analysis_input, sio_client):
        return {'resultList': []}

    monkeypatch.setattr(analyses, 'run', fake_run)
    monkeypatch.setattr('dokanalyse.processor.get_client', lambda: None)

    manager = RunningStatusTinyDBManager({
        'name': 'TinyDB',
        'connection': str(tmp_path / 'jobs.db'),
        'output_dir': str(tmp_path),
        'processes': {
            'dokanalyse': {'processor': {'name': 'dokanalyse.processor.DokanalyseProcessor'}}
        }
    })

    update_job = manager.update_job

    def flaky_update_job(job_id, update_dict):
        if update_dict.get('message') == 'Job running':
            raise RuntimeError('database unavailable')
        return update_job(job_id, update_dict)

    monkeypatch.setattr(manager, 'update_job', flaky_update_job)

    job_id, _, _, _, _ = manager.execute_process(
        'dokanalyse', {'inputGeometry': POLYGON_25833},
        execution_mode=RequestedProcessExecutionMode.respond_async)

    _wait_for_status(manager, job_id, 'successful')
