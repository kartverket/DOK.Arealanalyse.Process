import shutil
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict
import structlog
from structlog.stdlib import BoundLogger
from filelock import FileLock, AsyncFileLock
import asyncio
import aiohttp
import aiofiles

_CACHED_MARKER = ".cache_complete"
_LOCK_FILE = ".cache.lock"

_inproc_locks: Dict[Path, asyncio.Lock] = {}
_logger: BoundLogger = structlog.get_logger(__name__)


async def get_or_create_file(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    mapper: Callable[[Any], Awaitable[Any]] | None = None,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    path = path.resolve()

    if not with_lock:
        return await _get_or_create_file(url, path, session, mapper, semaphore)

    inproc_lock = _get_inproc_lock(path)
    lock_path = path.with_name(path.name + '.lock')
    lock = AsyncFileLock(lock_path, is_singleton=True)

    async with inproc_lock:
        async with lock:
            return await _get_or_create_file(url, path, session, mapper, semaphore)


def cache_dir(target_dir: Path, producer: Callable[[Path], None]) -> None:
    res_target_dir = target_dir.resolve()
    res_target_dir.mkdir(parents=True, exist_ok=True)

    marker_path = res_target_dir / _CACHED_MARKER
    lock_path = res_target_dir / _LOCK_FILE

    if marker_path.exists():
        return

    lock = FileLock(lock_path, is_singleton=True)

    with lock:
        if marker_path.exists():
            return

        tmp_dir = res_target_dir.with_name(res_target_dir.name + ".tmp")

        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

        tmp_dir.mkdir(parents=True, exist_ok=True)

        producer(tmp_dir)

        (tmp_dir / _CACHED_MARKER).touch()

        if res_target_dir.exists():
            shutil.rmtree(res_target_dir)

        tmp_dir.replace(res_target_dir)

        _logger.info('Directory cached', path=str(res_target_dir))


def should_refresh_cache(
    path: Path,
    *,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> bool:
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)

    max_age = timedelta(
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
    )

    return datetime.now(timezone.utc) - modified > max_age


async def _get_or_create_file(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    mapper: Callable[[Any], Awaitable[Any]] | None,
    semaphore: asyncio.Semaphore | None
) -> Path:
    if path.exists():
        return path
  
    if mapper:
        if semaphore:
            async with semaphore:
                return await _download_and_create_file_with_mapper(url, path, session, mapper)

        return await _download_and_create_file_with_mapper(url, path, session, mapper)

    if semaphore:
        async with semaphore:
            return await _download_and_create_file(url, path, session)

    return await _download_and_create_file(url, path, session)


async def _download_and_create_file_with_mapper(
    url: str,
    path: Path,
    session: aiohttp.ClientSession,
    mapper: Callable[[Any], Awaitable[Any]]
) -> Path:
    tmp_path = path.with_name(path.name + '.tmp')

    async with session.get(url) as response:
        response.raise_for_status()
        data: Dict[str, Any] = await response.json()

    mapped = await mapper(data)
    json_str = json.dumps(mapped, indent=2, ensure_ascii=False)

    async with aiofiles.open(path, 'w') as file:
        await file.write(json_str)

    tmp_path.replace(path)

    _logger.info('File cached', url=url, path=str(path))

    return path


async def _download_and_create_file(
        url: str,
        path: Path,
        session: aiohttp.ClientSession
) -> Path:
    tmp_path = path.with_name(path.name + '.tmp')

    async with session.get(url) as response:
        response.raise_for_status()

        async with aiofiles.open(tmp_path, 'wb') as file:
            async for chunk in response.content.iter_chunked(8192):
                await file.write(chunk)

    tmp_path.replace(path)

    _logger.info('File cached', url=url, path=str(path))

    return path


def _get_inproc_lock(path: Path) -> asyncio.Lock:
    lock = _inproc_locks.get(path)

    if lock is None:
        lock = asyncio.Lock()
        _inproc_locks[path] = lock

    return lock


__all__ = ['get_or_create_file', 'cache_dir', 'should_refresh_cache']
