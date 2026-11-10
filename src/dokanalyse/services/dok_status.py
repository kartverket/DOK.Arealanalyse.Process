from uuid import UUID
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from ..caching.dok_status import get_or_create_dok_status
from ..caching.json_cache import read_json
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_dok_status_for_dataset(metadata_id: UUID) -> Dict[str, Any] | None:
    dok_status = await _get_dok_status_by_dataset_id()

    return dok_status.get(str(metadata_id))


async def get_dok_status() -> List[Dict[str, Any]]:
    try:
        path = await get_or_create_dok_status(get_session())

        return read_json(path)
    except Exception as err:
        _logger.error('DOK-status download failed', error=str(err))
        return []


async def _get_dok_status_by_dataset_id() -> Dict[str, Dict[str, Any]]:
    try:
        path = await get_or_create_dok_status(get_session())

        return read_json(path, _index_by_dataset_id, key='by_dataset_id')
    except Exception as err:
        _logger.error('DOK-status download failed', error=str(err))
        return {}


def _index_by_dataset_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}

    for item in items:
        index.setdefault(item.get('dataset_id'), item)

    return index


__all__ = ['get_dok_status_for_dataset', 'get_dok_status']
