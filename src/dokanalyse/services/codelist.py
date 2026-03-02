import json
from typing import Any, Dict, List
import structlog
from structlog.stdlib import BoundLogger
from ..caching.codelist import get_or_create_codelist
from ..utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)

_codelists = {
    'arealressurs_arealtype': 'https://register.geonorge.no/api/sosi-kodelister/fkb/ar5/5.0/arealressursarealtype.json',
    'fullstendighet_dekning': 'https://register.geonorge.no/api/sosi-kodelister/temadata/fullstendighetsdekningskart/dekningsstatus.json',
    'vegkategori': 'https://register.geonorge.no/api/sosi-kodelister/kartdata/vegkategori.json'
}


async def get_codelist(type: str) -> List[Dict[str, Any]]:
    url = _codelists.get(type)

    if url is None:
        return []

    try:
        path = await get_or_create_codelist(url, type, get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error('Codelist download failed', url=url, error=str(err))
        return []


__all__ = ['get_codelist']
