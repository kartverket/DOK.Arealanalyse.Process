from pathlib import Path
from typing import Dict
import structlog
from structlog.stdlib import BoundLogger
from xmlschema import XMLSchema
from ..caching.xsd import get_or_create_xml_schema
from ..utils.http_context import get_session
from ..constants import CACHE_DIR

_logger: BoundLogger = structlog.get_logger(__name__)

_xsd_cache_dir = Path(CACHE_DIR).joinpath('xsds')
_xsd_cache_dir.mkdir(parents=True, exist_ok=True)

_gml_321_dir = _xsd_cache_dir.joinpath('gml_321/gml.xsd')
_xml_schema_cache: Dict[str, XMLSchema] = {}


async def compile_xml_schema(id: str, url: str) -> XMLSchema | None:
    filename = f'{id}.xsd'
    schema = _xml_schema_cache.get(filename)

    if schema:
        return schema

    try:
        path = await get_or_create_xml_schema(url, get_session())

        schema = XMLSchema([
            str(_gml_321_dir.resolve()),
            str(path.resolve())
        ])

        _xml_schema_cache[filename] = schema
        _logger.info('Compiled XML schema', url=url)

        return schema
    except Exception as err:
        _logger.error('XML Schema compilation failed',
                      id=id, url=url, error=str(err))
        return None


__all__ = ['compile_xml_schema']
