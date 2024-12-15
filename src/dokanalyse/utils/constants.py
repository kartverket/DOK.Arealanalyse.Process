from ..utils.helpers.common import get_env_var

APP_FILES_DIR = get_env_var('APP_FILES_DIR')
CACHE_DIR = f'{get_env_var("APP_FILES_DIR")}/cache'
AR5_FGDB_PATH = get_env_var('AR5_FGDB_PATH')
DEFAULT_EPSG = 25833
WGS84_EPSG = 4326