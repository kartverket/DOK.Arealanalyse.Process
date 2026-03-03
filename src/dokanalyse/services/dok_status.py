import json
from uuid import UUID
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from ..caching.dok_status import get_or_create_dok_status
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_dok_status_for_dataset(metadata_id: UUID) -> Dict[str, Any] | None:
    dok_status_all = await get_dok_status()

    for dok_status in dok_status_all:
        if dok_status.get('dataset_id') == str(metadata_id):
            return dok_status

    return None


async def get_dok_status() -> List[Dict[str, Any]]:
    try:
        path = await get_or_create_dok_status(get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error('DOK-status download failed', error=str(err))
        return []


__all__ = ['get_dok_status_for_dataset', 'get_dok_status']
