from pathlib import Path
from typing import Any, Dict, List
import asyncio
import aiohttp
from .caching import get_or_create_file, CacheUnit
from ..constants import CACHE_DIR

_API_BASE_URL = 'https://register.geonorge.no/api/det-offentlige-kartgrunnlaget-kommunalt.json?municipality='
_CACHE_DAYS = 7

_dok_datasets_cache_dir = Path(CACHE_DIR).joinpath('dok-datasets')
_dok_datasets_cache_dir.mkdir(parents=True, exist_ok=True)


async def get_or_create_kartgrunnlag(
    municipality_number: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    url = f'{_API_BASE_URL}{municipality_number}'
    path = _dok_datasets_cache_dir.joinpath(f'{municipality_number}.json')

    async def mapper(data: Dict[str, Any]) -> List[str]:
        return _map_data(data)

    return await get_or_create_file(
        url, 
        path, 
        session, 
        with_lock,
        mapper=mapper, 
        semaphore=semaphore,
        cache=(_CACHE_DAYS, CacheUnit.DAYS)
    )


def _map_data(data: Dict[str, Any]) -> List[str]:
    contained_items: List[Dict[str, Any]] = data.get('containeditems', [])
    datasets: List[str] = []

    for dataset in contained_items:
        if dataset['ConfirmedDok'] == 'JA':
            metadata_url: str = dataset['MetadataUrl']
            splitted = metadata_url.split('/')
            datasets.append(splitted[-1])

    return datasets


__all__ = ['get_or_create_kartgrunnlag']
