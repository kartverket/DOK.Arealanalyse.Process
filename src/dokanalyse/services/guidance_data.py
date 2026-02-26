import json
from uuid import UUID
from pathlib import Path
from typing import Any, Dict, List
import requests
import structlog
from structlog.stdlib import BoundLogger
from .caching import cache_file, should_refresh_cache
from ..constants import CACHE_DIR

_GEOLETT_API_URL = 'https://register.geonorge.no/geolett/api'
_CACHE_DAYS = 1

_logger: BoundLogger = structlog.get_logger(__name__)


def get_guidance_data(id: UUID) -> Dict[str, Any] | None:
    if id is None:
        return None

    guidance_data = _get_guidance_data()
    result = next(
        (item for item in guidance_data if str(id) == item['id']), None)

    return result


def _get_guidance_data() -> List[Dict[str, Any]]:
    file_path = Path(CACHE_DIR).joinpath('veiledningstekster.json')

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        try:
            def producer() -> str:
                guidance_data = _fetch_guidance_data()
                json_str = json.dumps(
                    guidance_data, indent=2, ensure_ascii=False)

                return json_str

            _ = cache_file(file_path, producer)
        except Exception as err:
            _logger.error('Veiledningstekster download failed', error=str(err))
            return []

    with file_path.open() as file:
        dok_status = json.load(file)

    return dok_status


def _fetch_guidance_data() -> Dict[str, Any]:
    response = requests.get(_GEOLETT_API_URL)
    response.raise_for_status()

    return response.json()


__all__ = ['get_guidance_data']
