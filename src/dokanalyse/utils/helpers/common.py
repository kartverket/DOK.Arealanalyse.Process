import re
import inspect
from os import environ
from typing import Dict, Any
from datetime import datetime, timezone
from lxml import etree as ET
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
    except:
        return None


def should_refresh_cache(file_path: str, cache_days: int) -> bool:
    timestamp = file_path.stat().st_mtime
    modified = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    diff = datetime.now(tz=timezone.utc) - modified

    return diff.days > cache_days


def xpath_select(element: ET._Element, path: str) -> Any:
    return element.xpath(path)


def xpath_select_one(element: ET._Element, path: str) -> Any:
    result = element.xpath(path)

    if len(result) == 0:
        return None

    if len(result) == 1:
        return result[0]

    raise Exception('Found more than one element')


def evaluate_condition(condition: str, data: Dict[str, Any]) -> bool:
    parsed_condition = _parse_condition(condition)
    result = eval(parsed_condition, data.copy())

    if isinstance(result, (bool)):
        return result

    raise Exception


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


def _parse_condition(condition: str) -> str:
    regex = r'(?<!=|>|<)\s*=\s*(?!=)'
    condition = re.sub(regex, ' == ', condition, 0, re.MULTILINE)

    return _replace_all(
        condition, {' AND ': ' and ', ' OR ': ' or ', ' IN ': ' in ', ' NOT ': ' not '})


def _replace_all(text: str, replacements: Dict) -> str:
    for i, j in replacements.items():
        text = text.replace(i, j)
    return text


__all__ = [
    'background_tasks',
    'get_env_var',
    'from_camel_case',
    'parse_string',
    'parse_date_string',
    'should_refresh_cache',
    'xpath_select',
    'xpath_select_one',
    'evaluate_condition',
    'objectify_properties'
    'dbg'
]
