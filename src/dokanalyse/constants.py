
from os import getenv, environ


def get_required_env(key: str) -> str:
    try:
        return environ[key]
    except KeyError:
        raise Exception(f'The environment variable {key} is not set')


APP_FILES_DIR: str = get_required_env('DOKANALYSE_APP_FILES_DIR')
DATASETS_CONFIG_DIR: str = get_required_env(
    'DOKANALYSE_DATASETS_CONFIG_DIR')
CACHE_DIR: str = f'{APP_FILES_DIR}/cache'
AR5_FGDB_PATH: str | None = getenv('DOKANALYSE_AR5_FGDB_PATH')
SOCKET_IO_SRV_URL: str | None = getenv('DOKANALYSE_SOCKET_IO_SRV_URL')
AZURE_BLOB_STORAGE_CONN_STR: str | None = getenv(
    'DOKANALYSE_AZURE_BLOB_STORAGE_CONN_STR')
LOCAL_FILE_SHARE_DIR: str | None = getenv(
    'DOKANALYSE_LOCAL_FILE_SHARE_DIR')
LOCAL_FILE_SHARE_BASE_URL: str | None = getenv(
    'DOKANALYSE_LOCAL_FILE_SHARE_BASE_URL')
PDF_TEMPLATES_DIR: str | None = getenv('DOKANALYSE_PDF_TEMPLATES_DIR')
CLIENT_TIMEOUT: int = int(getenv('DOKANALYSE_CLIENT_TIMEOUT', 30))
DATASETS: str | None = getenv('DOKANALYSE_DATASETS')
LOG_LEVEL: str = getenv('DOKANALYSE_LOG_LEVEL', 'INFO')
USE_XML_SCHEMAS: bool = (
    getenv('DOKANALYSE_USE_XML_SCHEMAS', 'False').lower() == 'true')
DEFAULT_EPSG: int = 25833
WGS84_EPSG: int = 4326
