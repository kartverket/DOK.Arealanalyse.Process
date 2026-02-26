import json
from pathlib import Path
import structlog
from structlog.stdlib import BoundLogger
from jsonschema import validate


_FILENAME = 'no.geonorge.dokanalyse.v1.input.schema.json'

_filepath = Path(__file__).parent.parent.parent.joinpath(
    'resources').joinpath(_FILENAME).resolve()

_logger: BoundLogger = structlog.get_logger(__name__)


def request_is_valid(data) -> bool:
    with _filepath.open() as file:
        schema = json.load(file)

    try:
        validate(instance=data, schema=schema)
        return True
    except Exception as err:
        _logger.error('Invalid request', error=str(err))
        return False


__all__ = ['request_is_valid']
