import json
from pathlib import Path
from typing import Any, Dict, List
import requests
import structlog
from structlog.stdlib import BoundLogger
from ..services.caching import cache_file, should_refresh_cache
from ..constants import CACHE_DIR

_CACHE_DAYS = 7

_logger: BoundLogger = structlog.get_logger(__name__)

_codelists = {
    'arealressurs_arealtype': 'https://register.geonorge.no/api/sosi-kodelister/fkb/ar5/5.0/arealressursarealtype.json',
    'fullstendighet_dekning': 'https://register.geonorge.no/api/sosi-kodelister/temadata/fullstendighetsdekningskart/dekningsstatus.json',
    'vegkategori': 'https://register.geonorge.no/api/sosi-kodelister/kartdata/vegkategori.json'
}


def get_codelist(type: str) -> List[Dict] | None:
    url = _codelists.get(type)

    if url is None:
        return None

    file_path = Path(CACHE_DIR).joinpath(f'codelists/{type}.json')

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        try:
            def producer() -> str:
                codelist = _get_codelist(url)
                json_str = json.dumps(codelist, indent=2, ensure_ascii=False)

                return json_str

            _ = cache_file(file_path, producer)
        except Exception as err:
            _logger.error('Codelist download failed', url=url, error=str(err))
            return None

    with file_path.open() as file:
        codelist = json.load(file)

    return codelist


def _get_codelist(url: str) -> List[Dict[str, Any]]:
    response = _fetch_codelist(url)
    contained_items: List[Dict[str, Any]] = response.get('containeditems', [])
    entries: List[Dict[str, Any]] = []

    for item in contained_items:
        if item.get('status') == 'Gyldig':
            entries.append({
                'value': item.get('codevalue'),
                'label': item.get('label'),
                'description': item.get('description')
            })

    return entries


def _fetch_codelist(url: str) -> Dict[str, Any]:
    response = requests.get(url)
    response.raise_for_status()

    return response.json()


__all__ = ['get_codelist']
