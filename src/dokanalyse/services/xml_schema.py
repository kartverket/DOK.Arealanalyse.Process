from pathlib import Path
from typing import Dict
import structlog
from structlog.stdlib import BoundLogger
import requests
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

_xsd_cache_dir = Path(CACHE_DIR).joinpath('xsds')
_xsd_cache_dir.mkdir(parents=True, exist_ok=True)


def cache_base_schemas() -> None:
    for name, url in _base_schemas:
        path = _xsd_cache_dir.joinpath(name)

        if path.exists():
            continue

        def producer(target: Path) -> None:
            schema = xmlschema.XMLSchema(url)
            schema.export(target, save_remote=True)

        try:            
            cache_dir(path, producer)
            _logger.info('Cached XML schema', url=url)
        except Exception as err:
            _logger.error('XML schema caching failed', url=url, error=str(err))


def compile_xml_schema(id: str, url: str) -> XMLSchema | None:
    filename = f'{id}.xsd'
    schema = _schema_cache.get(filename)

    if schema:
        return schema

    filepath = _xsd_cache_dir.joinpath(filename)

    try:
        if not filepath.exists():
            _save_xml_schema_to_disk(url, filepath)

        schema = xmlschema.XMLSchema([
            str(_xsd_cache_dir.joinpath('gml_321/gml.xsd').resolve()),
            str(filepath.resolve())
        ])

        _schema_cache[filename] = schema
        _logger.info('Compiled XML schema', url=url)

        return schema
    except Exception as err:
        _logger.error('XML Schema compilation failed',
                      id=id, url=url, error=str(err))
        return None


def _save_xml_schema_to_disk(url: str, filepath: Path) -> Path:
    def producer() -> bytes:
        response = requests.get(url)
        response.raise_for_status()

        return response.content

    return cache_file(filepath, producer)
