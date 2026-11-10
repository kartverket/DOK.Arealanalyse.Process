import json
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

# (path, key) -> (mtime_ns, size, value)
_cache: Dict[Tuple[Path, str], Tuple[int, int, Any]] = {}


def read_json(path: Path, transform: Callable[[Any], Any] | None = None, key: str = '') -> Any:
    """Reads and parses a JSON file, cached in memory until the file changes.

    `transform` is applied to the parsed data once per file version, e.g. to
    build a lookup index; use a distinct `key` per transform. The returned
    value is shared and must not be mutated.
    """
    path = path.resolve()
    stat = path.stat()
    cache_key = (path, key)
    cached = _cache.get(cache_key)

    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]

    with path.open(encoding='utf-8') as file:
        value = json.load(file)

    if transform:
        value = transform(value)

    _cache[cache_key] = (stat.st_mtime_ns, stat.st_size, value)

    return value


__all__ = ['read_json']
