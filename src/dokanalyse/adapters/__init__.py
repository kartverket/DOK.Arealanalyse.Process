import logging
import re
from os import getenv
from http import HTTPStatus
from typing import Tuple
import aiohttp
from pydantic import HttpUrl
from ..models.config.auth import Auth, ApiKey, Basic
from ..models.config.feature_service import FeatureService


_LOGGER = logging.getLogger(__name__)
_CREDENTIAL_REGEX = r"^\$\{(?P<env_var>.*?)\}$"


def log_error_response(url: HttpUrl, status_code: int) -> None:
    try:
        status_txt = HTTPStatus(status_code).phrase
    except ValueError:
        status_txt = 'Unknown status code'

    err = f'Error: {url}: {status_txt} ({status_code})'

    _LOGGER.error(err)


def get_service_credentials(service: HttpUrl | FeatureService) -> Tuple[str, Auth | None]:
    if isinstance(service, HttpUrl):
        return str(service), None

    return str(service.url), service.auth


def get_http_session(auth: Auth) -> aiohttp.ClientSession:
    headers = {}
    basic_auth: aiohttp.BasicAuth = None

    if isinstance(auth, ApiKey):
        headers['x-api-key'] = _get_credential(auth.api_key)
    elif isinstance(auth, Basic):
        basic_auth = aiohttp.BasicAuth(_get_credential(
            auth.username), _get_credential(auth.password))

    return aiohttp.ClientSession(headers=headers, auth=basic_auth)


def get_service_url(service: HttpUrl | FeatureService) -> str:
    if isinstance(service, HttpUrl):
        return str(service)

    return str(service.url)


def _get_credential(credential: str) -> str:
    match = re.search(_CREDENTIAL_REGEX, credential)

    if match:
        env_var = match.group('env_var')
        return getenv(env_var, credential)

    return credential


__all__ = ['log_error_response', 'get_service_credentials',
           'get_http_session', 'get_service_url']
