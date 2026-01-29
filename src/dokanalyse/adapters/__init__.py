import re
from os import getenv
from http import HTTPStatus
from typing import Tuple
import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from pydantic import HttpUrl
from ..models.config.auth import Auth, ApiKey, Basic
from ..models.config import DatasetConfig, FeatureService

_LOGGER: BoundLogger = structlog.get_logger(__name__)
_CREDENTIAL_REGEX = r"^\$\{(?P<env_var>.*?)\}$"


def log_http_error(resource: str, url: str, status_code: int, **kwargs) -> None:
    try:
        status_txt = HTTPStatus(status_code).phrase
    except ValueError:
        status_txt = 'Unknown status code'

    error_msg = f'Request to {resource} ({url}) failed: {status_code} {status_txt}'

    params = {
        'resource': resource,
        'url': url,
        'status_code': status_code,
        'message': error_msg
    }

    dataset: DatasetConfig = kwargs.pop('dataset', None)
    err: Exception = kwargs.pop('err', None)
    extra_params = {}
    
    if err:
        extra_params['error'] = str(err)

    if dataset:
        extra_params['config_id'] = str(dataset.config_id)
        extra_params['dataset'] = dataset.name

    params.update(extra_params)
    params.update(kwargs)

    _LOGGER.error('Request failed', **params)


def get_service_credentials(service: str | HttpUrl | FeatureService) -> Tuple[str, Auth | None]:
    if isinstance(service, str):
        return service, None

    if isinstance(service, HttpUrl):
        return str(service), None
   
    return str(service.url), service.auth


def get_http_session(auth: Auth) -> aiohttp.ClientSession:
    headers = {}
    basic_auth: aiohttp.BasicAuth = None

    if isinstance(auth, ApiKey):
        headers['x-api-key'] = get_credential(auth.api_key)
    elif isinstance(auth, Basic):
        basic_auth = aiohttp.BasicAuth(get_credential(
            auth.username), get_credential(auth.password))

    return aiohttp.ClientSession(headers=headers, auth=basic_auth)


def get_service_url(service: HttpUrl | FeatureService) -> str:
    if isinstance(service, HttpUrl):
        return str(service)

    return str(service.url)


def get_credential(credential: str) -> str:
    match = re.search(_CREDENTIAL_REGEX, credential)

    if match:
        env_var = match.group('env_var')
        return getenv(env_var, credential)

    return credential


__all__ = ['log_error_response', 'get_service_credentials',
           'get_http_session', 'get_service_url', 'get_credentials']
