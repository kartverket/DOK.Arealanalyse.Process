import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict
from filelock import FileLock
import asyncio
import aiohttp
import aiofiles


async def get_or_create_file(
        url: str,
        path: Path,
        session: aiohttp.ClientSession,
        with_lock: bool = True,
        mapper: Callable[[Any], Awaitable[Any]] | None = None,
        semaphore: asyncio.Semaphore | None = None
) -> Path:
    if not with_lock:
        return await _get_or_create_file(url, path, session, mapper, semaphore)

    lock = FileLock(path.with_suffix('.lock'))

    with lock:
        return await _get_or_create_file(url, path, session, mapper, semaphore)


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
    tmp_path = path.with_suffix('.tmp')

    async with session.get(url) as response:
        response.raise_for_status()
        data: Dict[str, Any] = await response.json()

    mapped = await mapper(data)
    json_str = json.dumps(mapped, indent=2, ensure_ascii=False)

    async with aiofiles.open(tmp_path, 'w') as file:
        await file.write(json_str)

    tmp_path.replace(path)

    return path


async def _download_and_create_file(
        url: str,
        path: Path,
        session: aiohttp.ClientSession
) -> Path:
    tmp_path = path.with_suffix('.tmp')

    async with session.get(url) as response:
        response.raise_for_status()

        async with aiofiles.open(tmp_path, 'wb') as file:
            async for chunk in response.content.iter_chunked(8192):
                await file.write(chunk)

    tmp_path.replace(path)

    return path


__all__ = ['get_or_create_file', 'should_refresh_cache']
