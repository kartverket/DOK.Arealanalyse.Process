import json
from uuid import UUID
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from ..caching.guidance_data import get_or_create_guidance_data
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_guidance_data(id: UUID | None) -> Dict[str, Any] | None:
    if id is None:
        return None

    guidance_data = await _get_guidance_data()

    result = next(
        (item for item in guidance_data if str(id) == item['id']), None)

    return result


async def _get_guidance_data() -> List[Dict[str, Any]]:
    try:
        path = await get_or_create_guidance_data(get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error('Guidance data download failed', error=str(err))
        return []


__all__ = ['get_guidance_data']
