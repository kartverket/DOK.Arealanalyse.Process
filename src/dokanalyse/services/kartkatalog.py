import json
from uuid import UUID
from typing import Any, Dict
import structlog
from structlog.stdlib import BoundLogger
from ..models.metadata import Metadata
from ..caching.kartkatalog import get_or_create_kartkatalog_metadata
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_kartkatalog_metadata(metadata_id: UUID | None) -> Metadata | None:
    if metadata_id is None:
        return None

    metadata = await _get_kartkatalog_metadata(metadata_id)

    if metadata is None:
        return None

    return Metadata.from_dict(metadata)


async def _get_kartkatalog_metadata(metadata_id: UUID) -> Dict[str, Any] | None:
    try:
        path = await get_or_create_kartkatalog_metadata(str(metadata_id), get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error('Kartkatalog metadata download failed',
                      metadata_id=str(metadata_id), error=str(err))
        return None


__all__ = ['get_kartkatalog_metadata']
