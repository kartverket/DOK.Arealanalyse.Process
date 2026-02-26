import json
from pathlib import Path
from uuid import UUID
from typing import Any, Dict
import requests
import structlog
from structlog.stdlib import BoundLogger
from ..models.metadata import Metadata
from ..services.caching import cache_file, should_refresh_cache
from ..constants import CACHE_DIR

_API_BASE_URL = 'https://kartkatalog.geonorge.no/api/getdata'
_CACHE_DAYS = 2

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_kartkatalog_metadata(metadata_id: UUID | None) -> Metadata | None:
    if metadata_id is None:
        return None

    metadata = await _get_kartkatalog_metadata(metadata_id)

    if metadata is None:
        return None

    return Metadata.from_dict(metadata)


async def _get_kartkatalog_metadata(metadata_id: UUID) -> Dict[str, Any] | None:
    file_path = Path(CACHE_DIR).joinpath(f'kartkatalog/{metadata_id}.json')

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        try:
            def producer() -> str:                
                response = _fetch_kartkatalog_metadata(metadata_id)               
                metadata = _map_response(metadata_id, response)
                json_str = json.dumps(metadata, indent=2, ensure_ascii=False)
                _logger.info('Kartkatalog metadata downloaded', metadata_id=str(metadata_id))

                return json_str

            _ = cache_file(file_path, producer)
        except Exception as err:
            _logger.error('Kartkatalog metadata download failed',
                          metadata_id=str(metadata_id), error=str(err))
            return None

    with file_path.open() as file:
        metadata = json.load(file)

    return metadata


def _map_response(metadata_id: UUID, response: Dict[str, Any]) -> Dict[str, Any]:
    title = response.get('NorwegianTitle')
    description = response.get('Abstract')
    owner = response.get('ContactOwner', {}).get('Organization')
    updated = response.get('DateUpdated')
    dataset_description_uri = f'https://kartkatalog.geonorge.no/metadata/{metadata_id}'

    return {
        'datasetId': str(metadata_id),
        'title': title,
        'description': description,
        'owner': owner,
        'updated': updated,
        'datasetDescriptionUri': dataset_description_uri
    }


def _fetch_kartkatalog_metadata(metadata_id: UUID) -> Dict[str, Any]:
    url = f'{_API_BASE_URL}/{metadata_id}'

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


__all__ = ['get_kartkatalog_metadata']
