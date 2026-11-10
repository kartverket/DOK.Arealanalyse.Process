from pathlib import Path
import hashlib
import asyncio
import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from xmlschema import XMLSchema
from .caching import get_or_create_file, cache_dir, CacheUnit
from ..constants import CACHE_DIR

_CACHE_DAYS = 30

XSD_CACHE_DIR = Path(CACHE_DIR).joinpath('xsds')

_base_xml_schemas = [
    ('gml_321', 'https://schemas.opengis.net/gml/3.2.1/gml.xsd')
]

_logger: BoundLogger = structlog.get_logger(__name__)


async def get_or_create_xml_schema(
    url: str,
    session: aiohttp.ClientSession,
    with_lock: bool = True,
    semaphore: asyncio.Semaphore | None = None
) -> Path:
    xsd_url = f'{url}?service=WFS&version=2.0.0&request=DescribeFeatureType'
    filename = f'{hash_url(url)}.xsd'
    XSD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = XSD_CACHE_DIR.joinpath(filename)

    return await get_or_create_file(
        xsd_url,
        path,
        session,
        with_lock,
        semaphore=semaphore,
        timeout=5,
        cache=(_CACHE_DAYS, CacheUnit.DAYS)
    )


def cache_base_xml_schemas() -> None:
    """Downloads base XML schemas (GML) with all imported schemas. Blocking."""
    for name, url in _base_xml_schemas:
        path = XSD_CACHE_DIR.joinpath(name)

        if path.exists():
            continue

        def producer(target: Path, schema_url: str = url) -> None:
            schema = XMLSchema(schema_url)
            schema.export(target, save_remote=True)

        try:
            cache_dir(path, producer)
        except Exception as err:
            _logger.error('Caching base XML schema failed', url=url, error=str(err))


def hash_url(url: str) -> str:
    return hashlib.sha256(url.encode('utf-8')).hexdigest()


__all__ = ['get_or_create_xml_schema', 'cache_base_xml_schemas', 'hash_url', 'XSD_CACHE_DIR']
