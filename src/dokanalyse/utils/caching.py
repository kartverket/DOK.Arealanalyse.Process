import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable
from filelock import FileLock


def save_cache(cache_root: Path, key: str, callback: Callable[..., None], **kwargs) -> Path:
    cache_dir = Path(cache_root)
    cache_dir.mkdir(parents=True, exist_ok=True)

    final_dir = cache_dir / key
    lock_path = cache_dir / f'{key}.lock'

    lock = FileLock(str(lock_path))

    with lock:
        marker = final_dir / '.complete'

        if marker.exists():
            return final_dir

        tmp_parent = cache_dir
        tmp_dir = Path(tempfile.mkdtemp(
            prefix=f'{key}.tmp.', dir=str(tmp_parent)))

        try:
            callback(tmp_dir, **kwargs)

            (tmp_dir / '.complete').write_text('ok', encoding='utf-8')

            if final_dir.exists():
                shutil.rmtree(final_dir)

            os.replace(tmp_dir, final_dir)

            return final_dir
        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise


__all__ = ['save_cache']
