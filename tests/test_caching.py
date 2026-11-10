import os
import time
import pytest
from dokanalyse.caching import caching
from dokanalyse.caching.caching import get_or_create_file, CacheUnit


class FakeResponse:
    def __init__(self, data=None, error: Exception | None = None):
        self._data = data
        self._error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self._error:
            raise self._error

    async def json(self, **kwargs):
        return self._data


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = 0

    def get(self, url, **kwargs):
        self.calls += 1
        return self.response


async def _mapper(data):
    return data


@pytest.fixture(autouse=True)
def clear_failed_refreshes():
    caching._failed_refreshes.clear()
    yield
    caching._failed_refreshes.clear()


async def test_downloads_and_caches(tmp_path):
    session = FakeSession(FakeResponse({'a': 1}))
    path = tmp_path.joinpath('sub', 'file.json')

    result = await get_or_create_file('https://x', path, session, mapper=_mapper, cache=(1, CacheUnit.DAYS))
    await get_or_create_file('https://x', path, session, mapper=_mapper, cache=(1, CacheUnit.DAYS))

    assert result.read_text().strip().startswith('{')
    assert session.calls == 1


async def test_returns_stale_file_when_refresh_fails(tmp_path):
    path = tmp_path.joinpath('file.json')
    path.write_text('{"stale": true}')
    old = time.time() - 3 * 86400
    os.utime(path, (old, old))

    session = FakeSession(FakeResponse(error=RuntimeError('service down')))

    result = await get_or_create_file('https://x', path, session, mapper=_mapper, cache=(1, CacheUnit.DAYS))
    await get_or_create_file('https://x', path, session, mapper=_mapper, cache=(1, CacheUnit.DAYS))

    assert result.read_text() == '{"stale": true}'
    assert session.calls == 1  # the second call does not retry immediately


async def test_raises_when_no_stale_file(tmp_path):
    session = FakeSession(FakeResponse(error=RuntimeError('service down')))

    with pytest.raises(RuntimeError):
        await get_or_create_file('https://x', tmp_path.joinpath('f.json'), session, mapper=_mapper)
