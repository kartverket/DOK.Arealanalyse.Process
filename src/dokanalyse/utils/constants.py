from os import getenv
from typing import Final
from ..utils.helpers.common import get_env_var


APP_FILES_DIR: Final[str] = get_env_var('DOKANALYSE_APP_FILES_DIR')
CACHE_DIR:  Final[str] = f'{APP_FILES_DIR}/cache'
AR5_FGDB_PATH: Final[str | None] = getenv('DOKANALYSE_AR5_FGDB_PATH')
SOCKET_IO_SRV_URL: Final[str | None] = getenv('DOKANALYSE_SOCKET_IO_SRV_URL')
BLOB_STORAGE_CONN_STR: Final[str | None] = getenv(
    'DOKANALYSE_BLOB_STORAGE_CONN_STR')
MAP_IMAGE_BASE_MAP: Final[str] = getenv(
    'DOKANALYSE_MAP_IMAGE_BASE_MAP', 'WMTS')
PDF_TEMPLATES_DIR: Final[str | None] = getenv('DOKANALYSE_PDF_TEMPLATES_DIR')
QUERY_TIMEOUT: Final[int] = int(getenv('DOKANALYSE_QUERY_TIMEOUT', 30))
DATASETS: Final[str | None] = getenv('DOKANALYSE_DATASETS')
LOG_LEVEL: Final[str] = getenv('DOKANALYSE_LOG_LEVEL', 'INFO')
DEFAULT_EPSG: Final[int] = 25833
WGS84_EPSG: Final[int] = 4326
