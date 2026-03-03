import os
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote
from typing import Any, Dict, List


def get_env_var(var_name) -> str:
    try:
        return os.environ[var_name]
    except KeyError:
        raise Exception(
            'The environment variable ' + var_name + ' is not set')


def file_url_to_path(url: str) -> Path | None:
    parsed = urlparse(url)

    if parsed.scheme != 'file':
        return None

    return Path(unquote(parsed.path))


def get_config_file_paths() -> List[Path]:
    config_dir = get_env_var('DOKANALYSE_DATASETS_CONFIG_DIR')

    if config_dir is None:
        raise Exception(
            'The environment variable "DOKANALYSE_DATASETS_CONFIG_DIR" is not set')

    path = Path(config_dir)

    if not path.is_dir():
        raise Exception(
            f'The "DOKANALYSE_DATASETS_CONFIG_DIR" path ({path}) is not a directory')

    glob = path.glob('*.yml')
    paths = [path for path in glob if path.is_file()]

    return paths


def from_camel_case(value):
    regex = r"([A-Z])"
    subst = " \\1"
    result = re.sub(regex, subst, value, 0)

    return result.capitalize()


def parse_string(value: str) -> str | int | float | bool:
    if value is None:
        return None

    if value.isdigit():
        return int(value)
    elif value.replace('.', '', 1).isdigit() and value.count('.') < 2:
        return float(value)
    elif value.lower() == True:
        return True
    elif value.lower() == False:
        return False

    return value


def parse_date_string(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except:
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


__all__ = [
    'get_env_var',
    'file_url_to_path',
    'from_camel_case',
    'parse_string',
    'parse_date_string',
    'objectify_properties',
]
