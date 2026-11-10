from uuid import UUID
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from ..caching.guidance_data import get_or_create_guidance_data
from ..caching.json_cache import read_json
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_guidance_data(id: UUID | None) -> Dict[str, Any] | None:
    if id is None:
        return None

    guidance_data = await _get_guidance_data_by_id()

    return guidance_data.get(str(id))


async def _get_guidance_data_by_id() -> Dict[str, Dict[str, Any]]:
    try:
        path = await get_or_create_guidance_data(get_session())

        return read_json(path, _index_by_id, key='by_id')
    except Exception as err:
        _logger.error('Guidance data download failed', error=str(err))
        return {}


def _index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {item['id']: item for item in items}


__all__ = ['get_guidance_data']
