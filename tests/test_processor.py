import time
import pytest
from pygeoapi.process.base import ProcessorExecuteError
from dokanalyse.processor import DokanalyseProcessor


def test_invalid_payload_is_rejected_with_reason():
    processor = DokanalyseProcessor({'name': 'dokanalyse'})

    with pytest.raises(ProcessorExecuteError, match='inputGeometry'):
        processor.execute({'requestedBuffer': 10})


def test_process_supports_async_execution():
    processor = DokanalyseProcessor({'name': 'dokanalyse'})

    assert 'async-execute' in processor.metadata['jobControlOptions']
    assert 'sync-execute' in processor.metadata['jobControlOptions']


def test_respond_async_runs_analysis_as_job(tmp_path, monkeypatch):
    from pygeoapi.process.manager.tinydb_ import TinyDBManager
    from pygeoapi.util import JobStatus, RequestedProcessExecutionMode
    from dokanalyse.services import analyses
    from conftest import POLYGON_25833

    async def fake_run(analysis_input, sio_client):
        return {'resultList': [], 'inputGeometryArea': 10000.0}

    monkeypatch.setattr(analyses, 'run', fake_run)
    monkeypatch.setattr('dokanalyse.processor.get_client', lambda: None)

    manager = TinyDBManager({
        'name': 'TinyDB',
        'connection': str(tmp_path / 'jobs.db'),
        'output_dir': str(tmp_path),
        'processes': {
            'dokanalyse': {'processor': {'name': 'dokanalyse.processor.DokanalyseProcessor'}}
        }
    })

    job_id, _, outputs, status, headers = manager.execute_process(
        'dokanalyse', {'inputGeometry': POLYGON_25833},
        execution_mode=RequestedProcessExecutionMode.respond_async)

    assert status == JobStatus.accepted
    assert outputs is None
    assert headers['Preference-Applied'] == 'respond-async'

    deadline = time.monotonic() + 10

    while manager.get_job(job_id)['status'] in ('accepted', 'running'):
        assert time.monotonic() < deadline, 'job did not finish'
        time.sleep(0.05)

    assert manager.get_job(job_id)['status'] == 'successful'
    assert manager.get_job_result(job_id) == (
        'application/json', {'resultList': [], 'inputGeometryArea': 10000.0})


def test_concurrent_analyses_are_limited(monkeypatch):
    import asyncio
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from dokanalyse import processor as processor_module
    from dokanalyse.services import analyses
    from conftest import POLYGON_25833

    running = 0
    max_running = 0

    async def fake_run(analysis_input, sio_client):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0.05)
        running -= 1
        return {}

    monkeypatch.setattr(analyses, 'run', fake_run)
    monkeypatch.setattr(processor_module, 'get_client', lambda: None)
    monkeypatch.setattr(processor_module, '_analysis_slots', threading.BoundedSemaphore(2))

    def execute(_):
        return DokanalyseProcessor({'name': 'dokanalyse'}).execute({'inputGeometry': POLYGON_25833})

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(execute, range(5)))

    assert len(results) == 5
    assert max_running == 2
