from pathlib import Path
from ..models.exceptions import DokAnalysisException
from typing import Any, Awaitable, Callable, Dict, List, Tuple
import typer
import asyncio
import aiohttp
from ..constants import DATASETS_CONFIG_DIR
import yaml
from .dok_status import get_or_create_dok_status
from .kartkatalog import get_or_create_kartkatalog_metadata
from .not_implemented_datasets import get_or_create_not_implemented_datasets
from .geofile import get_or_create_geofile
from .xsd import get_or_create_xml_schema
import json

app = typer.Typer()


@app.command()
def build_cache() -> None:
    asyncio.run(_run())


def main() -> None:
    app()


if __name__ == 'main':
    main()


async def _run() -> None:

    # wfs_urls, metadata_ids = _get_wfs_urls()
    # ids = list(set(metadata_ids))
    await _cache_resources()

    # print(len(metadata_ids))
    # print(len(ids))

    # await _cache_wfs_xml_schemas(ids)


async def _cache_resources() -> None:
    metadata_ids, wfs_urls, geojson_and_gpkg_urls = _get_config_data()

    timeout = aiohttp.ClientTimeout(total=30)

    connector = aiohttp.TCPConnector(
        limit=100, limit_per_host=10, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        dataset_ids = await _get_dataset_ids_from_dok_status(session)

        tasks: List[asyncio.Task] = []
        semaphore = asyncio.Semaphore(20)

        async with asyncio.TaskGroup() as tg:
            for dataset_id in dataset_ids:
                print(f'Caching kartkatalog metadata: {dataset_id}...')
                task = tg.create_task(_run_task(
                    get_or_create_kartkatalog_metadata, dataset_id, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)

            print('Caching non-implemented datasets...')
            tasks.append(tg.create_task(_run_task(get_or_create_not_implemented_datasets,
                         metadata_ids, session, with_lock=False, semaphore=semaphore)))

            for url in wfs_urls:
                print(f'Caching XML schema: {url}...')
                task = tg.create_task(_run_task(
                    get_or_create_xml_schema, url, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)

            for url in geojson_and_gpkg_urls:
                print(f'Caching files: {url}...')                
                task = tg.create_task(_run_task(
                    get_or_create_geofile, url, session, with_lock=False, semaphore=semaphore))
                tasks.append(task)

        # async with asyncio.TaskGroup() as tg:
        #     for dataset_id in dataset_ids:
        #         tasks.append(tg.create_task(get_or_create_kartkatalog_metadata(dataset_id, session, semaphore=semaphore)))


async def _run_task(func: Callable[..., Awaitable[Path]], *args, **kwargs) -> None:
    try:
        _ = await func(*args, **kwargs)
    except Exception as err:
        print(err)


async def _get_dataset_ids_from_dok_status(session: aiohttp.ClientSession) -> List[str]:
    path = await get_or_create_dok_status(session, with_lock=False)

    with path.open() as file:
        datasets: List[Dict[str, Any]] = json.loads(file.read())

    dataset_ids: List[str] = list(set([dataset['dataset_id']
                                       for dataset in datasets]))

    return dataset_ids


def _get_config_data() -> Tuple[List[str], List[str], List[str]]:
    path = Path(DATASETS_CONFIG_DIR)
    glob = path.glob('*.yml')
    paths = [path for path in glob if path.is_file()]

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
