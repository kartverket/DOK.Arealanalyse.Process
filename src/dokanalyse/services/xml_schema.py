import asyncio
from pathlib import Path
from typing import Dict
import structlog
from structlog.stdlib import BoundLogger
from xmlschema import XMLSchema
from ..caching.xsd import get_or_create_xml_schema, cache_base_xml_schemas, XSD_CACHE_DIR
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)

_gml_321_path = Path(XSD_CACHE_DIR).joinpath('gml_321/gml.xsd')
_xml_schema_cache: Dict[str, XMLSchema] = {}


async def compile_xml_schema(id: str, url: str) -> XMLSchema | None:
    schema = _xml_schema_cache.get(id)

    if schema:
        return schema

    try:
        if not _gml_321_path.exists():
            await asyncio.to_thread(cache_base_xml_schemas)

        path = await get_or_create_xml_schema(url, get_session())

        schema = await asyncio.to_thread(XMLSchema, [
            str(_gml_321_path.resolve()),
            str(path.resolve())
        ])

        _xml_schema_cache[id] = schema
        _logger.info('Compiled XML schema', url=url)

        return schema
    except Exception as err:
        _logger.error('XML Schema compilation failed',
                      id=id, url=url, error=str(err))
        return None


__all__ = ['compile_xml_schema']
