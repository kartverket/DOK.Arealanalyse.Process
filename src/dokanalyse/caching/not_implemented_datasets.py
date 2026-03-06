from pathlib import Path
from uuid import uuid4
from typing import Any, Dict, List
import asyncio
import aiohttp
from .caching import get_or_create_file, CacheUnit
from ..constants import CACHE_DIR

_API_URL = 'https://register.geonorge.no/api/dok-statusregisteret.json'
_FILENAME = 'not-implemented-datasets.json'
_CACHE_DAYS = 2

_codelists_cache_dir = Path(CACHE_DIR).joinpath('codelists')
_codelists_cache_dir.mkdir(parents=True, exist_ok=True)


async def get_or_create_not_implemented_datasets(
    metadata_ids: List[str],
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    path = Path(CACHE_DIR).joinpath(_FILENAME)

    async def mapper(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _map_data(data, metadata_ids)

    return await get_or_create_file(
        _API_URL,
        path,
        session,
        with_lock,
        mapper,
        semaphore,
        cache=(_CACHE_DAYS, CacheUnit.DAYS)
    )


def _map_data(
    data: Dict[str, Any],
    metadata_ids: List[str]
) -> List[Dict[str, Any]]:
    contained_items: List[Dict[str, Any]] = data.get('containeditems', [])
    configs: List[Dict[str, Any]] = []

    for item in contained_items:
        dataset_id = _get_dataset_id(item)

        if dataset_id in metadata_ids:
            continue

        name: str = item['seoname']
        theme: str = item.get('theme', '')

        configs.append({
            'config_id': str(uuid4()),
            'name': name.replace('-', '_'),
            'metadata_id': dataset_id,
            'themes': [theme] if theme else [],
        })

    return configs


def _get_dataset_id(item: Dict[str, Any]) -> str:
    metadata_url: str = item['MetadataUrl']
    dataset_id = metadata_url.split('/')[-1]

    return dataset_id


__all__ = ['get_or_create_not_implemented_datasets']
