from uuid import UUID
from pathlib import Path
import asyncio
import structlog
from structlog.stdlib import BoundLogger
import aiofiles
import xmlschema
from xmlschema import XMLSchema
from filelock import FileLock
from ..utils.event_loop_manager import get_session, get_semaphore
from ..utils.caching import save_cache
from ..utils.constants import CACHE_DIR

_logger: BoundLogger = structlog.get_logger(__name__)

_xsd_cache_dir = Path(f'{CACHE_DIR}/xsds')

_opengis_schemas = [
    ('gml_321', 'https://schemas.opengis.net/gml/3.2.1/gml.xsd')
]

if not _xsd_cache_dir.exists():
    _xsd_cache_dir.mkdir(parents=True)


def cache_opengis_schemas() -> None:
    for name, url in _opengis_schemas:
        path = _xsd_cache_dir.joinpath(name)

        if path.exists():
            continue

        try:
            _logger.info('Caching OpenGIS XML schema', url=url)
            save_cache(_xsd_cache_dir, name, _export_schema, url=url)
        except Exception as err:
            _logger.error('XML schema caching failed', url=url, error=str(err))


async def compile_xml_schema(id: str, url: str) -> XMLSchema | None:
    filename = f'{id}.xsd'
    filepath = _xsd_cache_dir.joinpath(filename)

    try:
        if not filepath.exists():
            await _cache_xml_schema(url, filepath)

        schema = xmlschema.XMLSchema([
            _xsd_cache_dir.joinpath('gml_321/gml.xsd'),
            filepath
        ])

        _logger.info(f'Compiled schema for {url}')
        return schema
    except Exception as err:
        _logger.error('XML Schema compilation failed',
                      id=id, url=url, error=str(err))
        return None


async def _cache_xml_schema(wfs_url: str, filepath: Path) -> None:
    xml_str = await _fetch_xml_schema(wfs_url)

    async with aiofiles.open(filepath, mode='w', encoding='utf-8') as file:
        await file.write(xml_str)


async def _fetch_xml_schema(url: str) -> str:
    async with get_semaphore():
        async with get_session().get(url) as response:
            response.raise_for_status()
            return await response.text()


def _export_schema(target: Path, **kwargs) -> None:
    schema = xmlschema.XMLSchema(kwargs['url'])
    schema.export(target, save_remote=True)
