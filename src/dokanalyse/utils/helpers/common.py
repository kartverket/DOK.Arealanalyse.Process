import re
from os import environ
from typing import List
from datetime import datetime, timezone
from lxml import etree as ET
from ...models.exceptions import DokAnalysisException


def get_env_var(var_name) -> str:
    try:
        return environ[var_name]
    except KeyError:
        raise DokAnalysisException('The environment variable ' + var_name + ' is not set')


def from_camel_case(value):
    regex = r"([A-Z])"
    subst = " \\1"
    result = re.sub(regex, subst, value, 0)

    return result.capitalize()


def to_camel_case(text: str) -> str:
    if text[0].islower():
        return text

    matches = re.findall('[A-ZÆØÅ][^A-ZÆØÅ]*', text)
    words: List[str] = list(map(lambda word: word.strip(' -_'), matches))
    result = words[0].lower() + ''.join(word.capitalize()
                                        for word in words[1:])

    return result


def keys_to_camel_case(data: dict) -> dict:
    return {to_camel_case(key): keys_to_camel_case(value) if isinstance(value, dict) else value for key, value in data.items()}


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


def xpath_select(element: ET._Element, path: str) -> any:
    return element.xpath(path)


def xpath_select_one(element: ET._Element, path: str) -> any:
    result = element.xpath(path)

    if len(result) == 0:
        return None

    if len(result) == 1:
        return result[0]

    raise Exception('Found more than one element')


def evaluate_condition(condition: str, data: dict[str, any]) -> bool:
    parsed_condition = _parse_condition(condition)
    result = eval(parsed_condition, data.copy())

    if isinstance(result, (bool)):
        return result

    raise Exception


def _parse_condition(condition: str) -> str:
    regex = r'(?<!=|>|<)\s*=\s*(?!=)'
    condition = re.sub(regex, ' == ', condition, 0, re.MULTILINE)

    return _replace_all(
        condition, {' AND ': ' and ', ' OR ': ' or ', ' IN ': ' in ', ' NOT ': ' not '})


def _replace_all(text: str, replacements: dict) -> str:
    for i, j in replacements.items():
        text = text.replace(i, j)
    return text


__all__ = [
    'background_tasks',
    'get_env_var', 
    'from_camel_case',
    'to_camel_case',
    'keys_to_camel_case',
    'parse_string',
    'parse_date_string',
    'should_refresh_cache',
    'xpath_select',
    'xpath_select_one',
    'evaluate_condition'
]