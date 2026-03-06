from enum import Enum
import shutil
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any, Awaitable, Callable, Dict, Tuple
import structlog
from structlog.stdlib import BoundLogger
from filelock import FileLock, AsyncFileLock
import asyncio
import aiohttp
import aiofiles

_CACHED_MARKER = '.cache_complete'
_LOCK_FILE = '.cache.lock'

_inproc_locks: Dict[Path, asyncio.Lock] = {}
_logger: BoundLogger = structlog.get_logger(__name__)


class CacheUnit(str, Enum):
    MONTHS = 'months'
    DAYS = 'days'
    HOURS = 'hours'
    MINUTES = 'minutes'
    SECONDS = 'seconds'


async def get_or_create_file(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    mapper: Callable[[Any], Awaitable[Any]] | None = None,
    semaphore: asyncio.Semaphore | None = None,
    timeout: int | None = None,
    cache: Tuple[int, CacheUnit] | None = None
) -> Path:
    path = path.resolve()

    if _is_cache_valid(path, cache):
        return path

    if not with_lock:
        return await _get_or_create_file(url, path, session, mapper, semaphore, timeout)

    lock_path = path.with_name(path.name + '.lock')
    inproc_lock = _get_inproc_lock(path)

    async with inproc_lock:
        if _is_cache_valid(path, cache):
            return path

        async with AsyncFileLock(lock_path, is_singleton=True):
            if _is_cache_valid(path, cache):
                return path
    
            return await _get_or_create_file(url, path, session, mapper, semaphore, timeout)


def cache_dir(
    target_dir: Path,
    producer: Callable[[Path], None],
    cache: Tuple[int, CacheUnit] | None = None
) -> None:
    target_dir = target_dir.resolve()

    if _is_cache_valid(target_dir, cache):
        return

    target_dir.mkdir(parents=True, exist_ok=True)

    marker_path = target_dir / _CACHED_MARKER
    lock_path = target_dir / _LOCK_FILE

    if marker_path.exists():
        return

    lock = FileLock(lock_path, is_singleton=True)

    with lock:
        if marker_path.exists():
            return

        tmp_dir = target_dir.with_name(target_dir.name + '.tmp')

        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

        tmp_dir.mkdir(parents=True, exist_ok=True)

        producer(tmp_dir)

        (tmp_dir / _CACHED_MARKER).touch()

        if target_dir.exists():
            shutil.rmtree(target_dir)

        tmp_dir.replace(target_dir)

        _logger.info('Directory cached', path=str(target_dir))


def should_refresh_cache(
    path: Path,
    value: int,
    unit: CacheUnit
) -> bool:
    modified = datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    cache_unit = str(unit.value).lower()

    if cache_unit == 'months':
        expiry = modified + relativedelta(months=value)
    else:
        timedelta_units = {
            'days': 'days',
            'hours': 'hours',
            'minutes': 'minutes',
            'seconds': 'seconds',
        }

        if cache_unit not in timedelta_units:
            raise ValueError(f'Unsupported unit: {cache_unit}')

        delta = timedelta(**{timedelta_units[cache_unit]: value})
        expiry = modified + delta

    return now > expiry


async def _get_or_create_file(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    mapper: Callable[[Any], Awaitable[Any]] | None,
    semaphore: asyncio.Semaphore | None,
    timeout: int | None
) -> Path:
    if mapper:
        if semaphore:
            async with semaphore:
                return await _download_and_create_file_with_mapper(url, path, session, mapper, timeout)

        return await _download_and_create_file_with_mapper(url, path, session, mapper, timeout)

    if semaphore:
        async with semaphore:
            return await _download_and_create_file(url, path, session, timeout)

    return await _download_and_create_file(url, path, session, timeout)


async def _download_and_create_file_with_mapper(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    mapper: Callable[[Any], Awaitable[Any]],
    timeout: int | None
) -> Path:
    tmp_path = path.with_name(path.name + '.tmp')
    request_args = _get_request_args(timeout)

    async with session.get(url, **request_args) as response:
        response.raise_for_status()
        data: Dict[str, Any] = await response.json()

    mapped = await mapper(data)
    json_str = json.dumps(mapped, indent=2, ensure_ascii=False)

    async with aiofiles.open(tmp_path, 'w') as file:
        await file.write(json_str)

    tmp_path.replace(path)

    _logger.info('File cached', url=url, path=str(path))

    return path


async def _download_and_create_file(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    timeout: int | None
) -> Path:
    tmp_path = path.with_name(path.name + '.tmp')
    request_args = _get_request_args(timeout)

    async with session.get(url, **request_args) as response:
        response.raise_for_status()

        async with aiofiles.open(tmp_path, 'wb') as file:
            async for chunk in response.content.iter_chunked(8192):
                await file.write(chunk)

    tmp_path.replace(path)

    _logger.info('File cached', url=url, path=str(path))

    return path


def _is_cache_valid(path: Path, cache: Tuple[int, CacheUnit] | None) -> bool:
    if not path.exists():
        return False

    if cache is None:
        return True

    return not should_refresh_cache(path, *cache)


def _get_inproc_lock(path: Path) -> asyncio.Lock:
    lock = _inproc_locks.get(path)

    if lock is None:
        lock = asyncio.Lock()
        _inproc_locks[path] = lock

    return lock


def _get_request_args(timeout: int | None) -> Dict[str, Any]:
    args: Dict[str, Any] = {}

    if timeout is not None and timeout > 0:
        args['timeout'] = aiohttp.ClientTimeout(total=timeout)

    return args


__all__ = ['get_or_create_file', 'cache_dir',
           'should_refresh_cache', 'CacheUnit']
