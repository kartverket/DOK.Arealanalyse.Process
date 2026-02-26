import re
from os import environ
from typing import Dict, Any
from datetime import datetime
from ...models.exceptions import DokAnalysisException


def get_env_var(var_name) -> str:
    try:
        return environ[var_name]
    except KeyError:
        raise DokAnalysisException(
            'The environment variable ' + var_name + ' is not set')


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


def parse_date_string(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except Exception:
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
    'from_camel_case',
    'parse_string',
    'parse_date_string',
    'objectify_properties',
]
