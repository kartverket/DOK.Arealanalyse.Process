import inspect
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote
from typing import Any, Dict, List
from ...constants import DATASETS_CONFIG_DIR

_number_regex = re.compile(r'^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$')


def file_url_to_path(url: str) -> Path | None:
    parsed = urlparse(url)

    if parsed.scheme != 'file':
        return None

    return Path(unquote(parsed.path))


def get_config_file_paths() -> List[Path]:
    path = Path(DATASETS_CONFIG_DIR)

    if not path.is_dir():
        raise Exception(
            f'The "DOKANALYSE_DATASETS_CONFIG_DIR" path ({path}) is not a directory')

    glob = path.glob('*.yml')
    paths = [path for path in glob if path.is_file()]

    return paths


def parse_string(value: str | None) -> str | int | float | bool | None:
    if value is None:
        return None

    lowered = value.strip().lower()

    if lowered == 'true':
        return True

    if lowered == 'false':
        return False

    number = parse_number(value)

    return value if number is None else number


def parse_number(value: str) -> int | float | None:
    text = value.strip()

    if not _number_regex.match(text):
        return None

    try:
        return int(text)
    except ValueError:
        return float(text)


def parse_date_string(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def objectify_properties(properties: Dict[str, Any]) -> Dict[str, Any]:
    result = {}

    for key, value in properties.items():
        parts = key.split('.')
        current = result

        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], Dict):
                current[part] = {}

            current = current[part]

        current[parts[-1]] = value

    return result


def dbg(*args, **kwargs) -> None:
    frame = inspect.stack()[1]
    filename = frame.filename
    lineno = frame.lineno

    print(f"{filename}:{lineno} —", *args, **kwargs)


__all__ = [
    'file_url_to_path',
    'parse_string',
    'parse_number',
    'parse_date_string',
    'objectify_properties',
    'dbg'
]
