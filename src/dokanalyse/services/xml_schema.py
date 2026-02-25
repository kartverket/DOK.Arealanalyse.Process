from pathlib import Path
from typing import Dict
import structlog
from structlog.stdlib import BoundLogger
import xmlschema
from xmlschema import XMLSchema
from .caching import cache_file, cache_dir
from ..utils.event_loop_manager import get_session, get_semaphore
from ..constants import CACHE_DIR

_logger: BoundLogger = structlog.get_logger(__name__)

_base_schemas = [
    ('gml_321', 'https://schemas.opengis.net/gml/3.2.1/gml.xsd')
]

_schema_cache: Dict[str, XMLSchema] = {}

_xsd_cache_dir = Path(f'{CACHE_DIR}/xsds')

if not _xsd_cache_dir.exists():
    _xsd_cache_dir.mkdir(parents=True)


def cache_base_schemas() -> None:
    for name, url in _base_schemas:
        path = _xsd_cache_dir.joinpath(name)

        if path.exists():
            continue

        def producer(target: Path) -> None:
            schema = xmlschema.XMLSchema(url)
            schema.export(target, save_remote=True)

        try:
            _logger.info('Caching XML schema', url=url)
            cache_dir(path, producer)
        except Exception as err:
            _logger.error('XML schema caching failed', url=url, error=str(err))


async def compile_xml_schema(id: str, url: str) -> XMLSchema | None:
    filename = f'{id}.xsd'
    schema = _schema_cache.get(filename)

    if schema:
        return schema

    filepath = _xsd_cache_dir.joinpath(filename)

    try:
        if not filepath.exists():
            await _save_xml_schema_to_disk(url, filepath)

        schema = xmlschema.XMLSchema([
            str(_xsd_cache_dir.joinpath('gml_321/gml.xsd').resolve()),
            str(filepath.resolve())
        ])

        _schema_cache[filename] = schema
        _logger.info(f'Compiled schema for {url}')

        return schema
    except Exception as err:
        _logger.error('XML Schema compilation failed',
                      id=id, url=url, error=str(err))
        return None


async def _save_xml_schema_to_disk(url: str, filepath: Path) -> Path:
    async def producer() -> bytes:
        async with get_semaphore():
            async with get_session().get(url) as response:
                response.raise_for_status()
                return await response.read()

    return await cache_file(filepath, producer)
