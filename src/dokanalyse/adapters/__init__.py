import logging
from http import HTTPStatus
from pydantic import HttpUrl

_LOGGER = logging.getLogger(__name__)


def log_error_response(url: HttpUrl, status_code: int) -> None:
    try:
        status_txt = HTTPStatus(status_code).phrase
    except ValueError:
        status_txt = 'Unknown status code'

    err = f'Error: {url}: {status_txt} ({status_code})'

    _LOGGER.error(err)


__all__ = ['log_error_response']