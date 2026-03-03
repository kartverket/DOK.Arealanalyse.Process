import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Tuple
import typer
import yaml
import asyncio
import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from .dok_status import get_or_create_dok_status
from .geofile import get_or_create_geofile
from .guidance_data import get_or_create_guidance_data
from .kartkatalog import get_or_create_kartkatalog_metadata
from .not_implemented_datasets import get_or_create_not_implemented_datasets
from .xsd import get_or_create_xml_schema, cache_base_xml_schemas
from ..utils.logger import setup as setup_logger
from ..utils.helpers.common import get_config_file_paths
from ..utils.async_executor import exec_async


setup_logger()

app = typer.Typer()

_logger: BoundLogger = structlog.get_logger(__name__)


@app.command()
def build_cache() -> None:
    exec_async(_cache_resources())


def main() -> None:
    app()


async def _cache_resources() -> None:
    _logger.info('Caching resources...')

    cache_base_xml_schemas()

    metadata_ids, wfs_urls, geojson_and_gpkg_urls = _get_config_data()
    timeout = aiohttp.ClientTimeout(total=30)

    connector = aiohttp.TCPConnector(
        limit=100, limit_per_host=10, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        dataset_ids = await _get_dataset_ids_from_dok_status(session)
        tasks: List[asyncio.Task] = []
        semaphore = asyncio.Semaphore(25)

        async with asyncio.TaskGroup() as tg:
            for url in wfs_urls:
                task = tg.create_task(_run_task(
                    get_or_create_xml_schema, url, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)

            for dataset_id in dataset_ids:
                task = tg.create_task(_run_task(
                    get_or_create_kartkatalog_metadata, dataset_id, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)

            tasks.append(tg.create_task(_run_task(get_or_create_not_implemented_datasets,
                         metadata_ids, session, with_lock=False, semaphore=semaphore)))

            tasks.append(tg.create_task(_run_task(
                get_or_create_guidance_data, session, with_lock=False, semaphore=semaphore)))

            for url in geojson_and_gpkg_urls:
                task = tg.create_task(_run_task(
                    get_or_create_geofile, url, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)


async def _run_task(func: Callable[..., Awaitable[Path]], *args, **kwargs) -> None:
    try:
        _ = await func(*args, **kwargs)
    except:
        pass


async def _get_dataset_ids_from_dok_status(session: aiohttp.ClientSession) -> List[str]:
    try:
        path = await get_or_create_dok_status(session, with_lock=False)

        with path.open() as file:
            datasets: List[Dict[str, Any]] = json.loads(file.read())

        dataset_ids: List[str] = [dataset['dataset_id'] for dataset in datasets]

        return list(set(dataset_ids))
    except:
        return []


def _get_config_data() -> Tuple[List[str], List[str], List[str]]:
    paths = get_config_file_paths()
    metadata_ids: List[str] = []
    wfs_urls: List[str] = []
    geojson_and_gpkg_urls: List[str] = []

    for path in paths:
        results = yaml.safe_load_all(path.read_text(encoding='utf-8'))
        result: Dict[str, Any]

        for result in results:
            if not result or result.get('disabled'):
                continue

            type: str = result['type']

            if type == 'dataset':
                metadata_id: str | None = result.get('metadata_id')

                if metadata_id:
                    metadata_ids.append(metadata_id)

                wfs_url = _get_wfs_url_from_dataset(result)

                if wfs_url:
                    wfs_urls.append(wfs_url)
            elif type == 'quality':
                indicators: List[Dict[str, Any]] = result.get('indicators', [])

                for indicator in indicators:
                    url = _get_url_from_indicator(indicator)

                    if url:
                        geojson_and_gpkg_urls.append(url)

    return list(set(metadata_ids)), list(set(wfs_urls)), list(set(geojson_and_gpkg_urls))


def _get_wfs_url_from_dataset(dataset: Dict[str, Any]) -> str | None:
    wfs: str | Dict[str, Any] | None = dataset.get('wfs')

    if isinstance(wfs, str) and _is_http_url(wfs):
        return wfs

    if isinstance(wfs, dict):
        url = wfs.get('url')

        if isinstance(url, str) and _is_http_url(url):
            return url

    return None


def _get_url_from_indicator(indicator: Dict[str, Any]) -> str | None:
    service: Dict[str, Any] | None = indicator.get(
        'geojson') or indicator.get('gpkg')

    if service and _is_http_url(service['url']):
        return service['url']

    return None


def _is_http_url(value: str) -> bool:
    return value.startswith('https:') or value.startswith('http:')
