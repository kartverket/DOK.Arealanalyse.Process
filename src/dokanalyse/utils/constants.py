from os import getenv
from typing import Final
from ..utils.helpers.common import get_env_var

APP_FILES_DIR: Final[str] = get_env_var('APP_FILES_DIR')
CACHE_DIR:  Final[str] = f'{APP_FILES_DIR}/cache'
AR5_FGDB_PATH: Final[str] = getenv('AR5_FGDB_PATH')
SOCKET_IO_SRV_URL: Final[str] = getenv('SOCKET_IO_SRV_URL')
BLOB_STORAGE_CONN_STR: Final[str] = getenv('BLOB_STORAGE_CONN_STR')
MAP_IMAGE_API_URL: Final[str] = getenv('MAP_IMAGE_API_URL')
MAP_IMAGE_BASE_MAP: Final[str] = getenv('MAP_IMAGE_BASE_MAP') or 'WMTS'
DEFAULT_EPSG: Final[int] = 25833
WGS84_EPSG: Final[int] = 4326
