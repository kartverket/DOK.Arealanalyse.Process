import json
import re
from typing import Any, Dict, List

_DATE_RE = re.compile(
    r"""
    ^
    \b
    (?P<year>(?:19|20)\d{2}) /
    (?P<month>0[1-9]|1[0-2]) /
    (?P<day>0[1-9]|[12]\d|3[01])
    (?:\s*(?P<tz_sign>[+-])
    (?P<tz_hour>0\d|1\d|2[0-3]) :
    (?P<tz_minute>[0-5]\d)
    )?
    \b
    $
    """,
    re.VERBOSE
)

_DATETIME_RE = re.compile(
    r"""
    \b
    ^
    (?P<year>(?:19|20)\d{2}) /
    (?P<month>0[1-9]|1[0-2]) /
    (?P<day>0[1-9]|[12]\d|3[01])
    \s+
    (?P<hour>[01]\d|2[0-3]) :
    (?P<minute>[0-5]\d) :
    (?P<second>[0-5]\d)
    (?:\.(?P<fraction>\d+))?
    (?:\s*(?P<tz_sign>[+-])
    (?P<tz_hour>0\d|1\d|2[0-3]) :
    (?P<tz_minute>[0-5]\d)
    )?
    \b
    $
    """,
    re.VERBOSE
)


def normalize_object(obj) -> Dict[str, Any]:
    if isinstance(obj, Dict):
        return {
            key: normalize_object(_normalize_string(value)
                           if isinstance(value, str) else value)
            for key, value in obj.items()
        }

    if isinstance(obj, List):
        return [
            normalize_object(_normalize_string(item)
                      if isinstance(item, str) else item)
            for item in obj
        ]

    return obj


def _normalize_string(value: str) -> Any:
    if not isinstance(value, str):
        return value

    if value.startswith('{') and value.endswith('}'):
        try:
            parsed = json.loads(value)

            if isinstance(parsed, (Dict, List)):
                return parsed
        except json.JSONDecodeError:
            pass

    match = _DATE_RE.search(value)

    if match:
        return _parse_date(match.groupdict())

    match = _DATETIME_RE.search(value)

    if match:
        return _parse_datetime(match.groupdict())

    return value


def _parse_date(gd: Dict[str, str | Any]) -> str:
    date_str = f'{gd['year']}-{gd['month']}-{gd['day']}'

    return date_str + _parse_timezone(gd)


def _parse_datetime(gd: Dict[str, str | Any]) -> str:
    datetime_str = f'{gd['year']}-{gd['month']}-{gd['day']}T{gd['hour']}:{gd['minute']}:{gd['second']}'

    return datetime_str + _parse_timezone(gd)


def _parse_timezone(gd: Dict[str, str | Any]) -> str:
    tz_sign = gd.get('tz_sign')

    if not tz_sign:
        return ''

    tz_hour = gd['tz_hour']
    tz_minute = gd.get('tz_minute')

    return f'{tz_sign}{tz_hour}:{tz_minute or "00"}'


__all__ = ['normalize_object']