import os
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Awaitable, Callable


class CacheLockTimeout(Exception):
    ...


async def cache_file(
    target: Path,
    producer: Callable[..., Awaitable[bytes | str]],
    *,
    encoding: str = 'utf-8',
    timeout_sec: float = 300.0,
    poll_sec: float = 0.2,
    stale_after_sec: float = 3600.0,
) -> Path:
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)

    lockdir = target.with_name(target.name + '.lockdir')
    start = time.time()

    while True:
        try:
            lockdir.mkdir()
            break
        except FileExistsError:
            try:
                if time.time() - lockdir.stat().st_mtime > stale_after_sec:
                    for path in lockdir.iterdir():
                        try:
                            path.unlink()
                        except OSError:
                            pass
                    try:
                        lockdir.rmdir()
                    except OSError:
                        pass
                    continue
            except FileNotFoundError:
                continue

            if time.time() - start > timeout_sec:
                raise CacheLockTimeout(f'Timeout waiting for {lockdir}')
            time.sleep(poll_sec)

    try:
        if target.exists():
            return target

        fd, tmp_name = tempfile.mkstemp(
            prefix=target.name + '.tmp.', dir=str(target.parent))
        tmp = Path(tmp_name)

        try:
            payload = await producer()
            data = payload.encode(encoding) if isinstance(
                payload, str) else payload

            with os.fdopen(fd, 'wb') as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())

            os.replace(tmp, target)

            return target
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
    finally:
        try:
            lockdir.rmdir()
        except OSError:
            pass


def cache_dir(
    target_dir: Path,
    producer: Callable[[Path], None],
    *,
    timeout_sec: float = 300.0,
    poll_sec: float = 0.2,
    stale_after_sec: float = 3600.0,
    marker_name: str = '.complete',
    overwrite: bool = False,
) -> Path:
    target_dir = Path(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    marker = target_dir / marker_name
    lockdir = target_dir.with_name(target_dir.name + '.lockdir')
    start = time.time()

    while True:
        try:
            lockdir.mkdir()
            break
        except FileExistsError:
            try:
                if time.time() - lockdir.stat().st_mtime > stale_after_sec:
                    for path in lockdir.iterdir():
                        try:
                            if path.is_dir():
                                shutil.rmtree(path, ignore_errors=True)
                            else:
                                path.unlink(missing_ok=True)
                        except OSError:
                            pass
                    try:
                        lockdir.rmdir()
                    except OSError:
                        pass
                    continue
            except FileNotFoundError:
                continue

            if time.time() - start > timeout_sec:
                raise CacheLockTimeout(f'Timeout waiting for lock: {lockdir}')
            time.sleep(poll_sec)

    try:
        if not overwrite and marker.exists():
            return target_dir

        tmp_parent = target_dir.parent
        tmp_dir = Path(tempfile.mkdtemp(
            prefix=target_dir.name + '.tmp.', dir=str(tmp_parent)))

        try:
            producer(tmp_dir)
            (tmp_dir / marker_name).write_text('ok', encoding='utf-8')

            if target_dir.exists():
                shutil.rmtree(target_dir)

            os.replace(tmp_dir, target_dir)
            return target_dir

        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    finally:
        try:
            lockdir.rmdir()
        except OSError:
            pass


def should_refresh_cache(file_path: Path, cache_days: int) -> bool:
    timestamp = file_path.stat().st_mtime
    modified = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    diff = datetime.now(tz=timezone.utc) - modified

    return diff.days > cache_days


__all__ = ['cache_file', 'cache_dir', 'should_refresh_cache']
