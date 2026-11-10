import json
from pathlib import Path
from typing import Any
from jsonschema import Draft7Validator

_FILENAME = 'no.geonorge.dokanalyse.v1.input.schema.json'

_filepath = Path(__file__).parent.parent.parent.joinpath(
    'resources').joinpath(_FILENAME).resolve()

# The schema is self-contained (no remote $refs), so it is compiled once.
_validator = Draft7Validator(json.loads(_filepath.read_text(encoding='utf-8')))


def get_request_validation_error(data: Any) -> str | None:
    error = next(iter(sorted(_validator.iter_errors(data), key=lambda err: err.path)), None)

    if error is None:
        return None

    path = '/'.join(str(part) for part in error.absolute_path)

    return f'{path}: {error.message}' if path else error.message


__all__ = ['get_request_validation_error']
