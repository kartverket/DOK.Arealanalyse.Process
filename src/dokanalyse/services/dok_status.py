import json
from uuid import UUID
from pathlib import Path
from typing import Any, Dict, List, Tuple
import structlog
from structlog.stdlib import BoundLogger
from ..services.caching import cache_file, should_refresh_cache
from ..utils.event_loop_manager import get_session, get_semaphore
from ..constants import CACHE_DIR

_API_URL = 'https://register.geonorge.no/api/dok-statusregisteret.json'
_CACHE_DAYS = 2

_logger: BoundLogger = structlog.get_logger(__name__)

_category_mappings = {
    'BuildingMatter': ('egnethet_byggesak', 'Byggesak'),
    'MunicipalLandUseElementPlan': ('egnethet_kommuneplan', 'Kommuneplan'),
    'ZoningPlan': ('egnethet_reguleringsplan', 'Reguleringsplan')
}

_value_mappings = {
    0: 'Ikke egnet',
    1: 'Dårlig egnet',
    2: 'Noe egnet',
    3: 'Egnet',
    4: 'Godt egnet',
    5: 'Svært godt egnet'
}


async def get_dok_status_for_dataset(metadata_id: UUID) -> Dict[str, Any] | None:
    dok_status_all = await get_dok_status()

    for dok_status in dok_status_all:
        if dok_status.get('dataset_id') == str(metadata_id):
            return dok_status

    return None


async def get_dok_status() -> List[Dict[str, Any]]:
    file_path = Path(CACHE_DIR).joinpath('dok-status.json')

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        try:
            async def producer() -> str:
                dok_status = await _get_dok_status()
                json_str = json.dumps(dok_status, indent=2, ensure_ascii=False)

                return json_str

            _ = await cache_file(file_path, producer)
        except Exception as err:
            _logger.error('DOK-status download failed', error=str(err))
            return []

    with file_path.open() as file:
        dok_status = json.load(file)

    return dok_status


async def _get_dok_status() -> List[Dict]:
    response = await _fetch_dok_status()
    contained_items: List[Dict[str, Any]] = response.get('containeditems', [])
    datasets: List[Dict[str, Any]] = []

    for item in contained_items:
        dataset_id = _get_dataset_id(item)
        theme: str = item.get('theme', '')
        categories = _get_relevant_categories(item)
        suitability = []

        for key, value in categories:
            id, name = _category_mappings.get(key)

            suitability.append({
                'quality_dimension_id': id,
                'quality_dimension_name': name,
                'value': value,
                'comment': _value_mappings.get(value)
            })

        datasets.append({
            'dataset_id': dataset_id,
            'theme': theme,
            'suitability': suitability
        })

    return datasets


async def _fetch_dok_status() -> Dict[str, Any]:
    async with get_semaphore():
        async with get_session().get(_API_URL) as response:
            response.raise_for_status()
            return await response.json()


def _get_dataset_id(item: Dict[str, Any]) -> str:
    metadata_url: str = item['MetadataUrl']
    dataset_id = metadata_url.split('/')[-1]

    return dataset_id


def _get_relevant_categories(item: Dict[str, Any]) -> List[Tuple[str, str]]:
    suitability: Dict[str, str] = item['Suitability']
    categories = [(key, value) for key, value in suitability.items()
                  if key in _category_mappings.keys()]

    return categories


__all__ = ['get_dok_status_for_dataset', 'get_dok_status']
