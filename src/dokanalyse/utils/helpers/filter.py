from typing import Any, Callable, Dict
from pygeofilter.parsers.cql2_text import parse
from pygeofilter.backends.native.evaluate import NativeEvaluator

FilterFunc = Callable[[Dict[str, Any]], bool]


def compile_cql_filter(cql_text: str) -> FilterFunc:
    ast: Any = parse(cql_text)

    return NativeEvaluator(use_getattr=False).evaluate(ast)


def evaluate_filter(filter_func: FilterFunc, properties: Dict[str, Any]) -> bool:
    """Evaluates a compiled CQL filter against feature properties.

    Values parsed from XML are strings, so a filter such as
    "gjentaksintervall IN (20, 200)" would never match "200". If the filter
    does not match the raw values, it is evaluated once more with numeric
    strings converted to numbers. Filters written with string literals
    ("kps IN ('20', '40')") therefore keep working.
    """
    if _safe_evaluate(filter_func, properties):
        return True

    coerced = {key: _to_number(value) for key, value in properties.items()}

    if coerced == properties:
        return False

    return _safe_evaluate(filter_func, coerced)


def _safe_evaluate(filter_func: FilterFunc, properties: Dict[str, Any]) -> bool:
    try:
        return bool(filter_func(properties))
    except Exception:
        return False


def _to_number(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    text = value.strip()

    try:
        return int(text)
    except ValueError:
        pass

    try:
        return float(text)
    except ValueError:
        return value


__all__ = ['FilterFunc', 'compile_cql_filter', 'evaluate_filter']
