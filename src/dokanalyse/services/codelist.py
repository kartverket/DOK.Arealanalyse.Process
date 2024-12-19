from os import path
from pathlib import Path
import json
from typing import List, Dict
import aiohttp
from ..utils.constants import CACHE_DIR
from ..utils.helpers.common import should_refresh_cache

_CACHE_DAYS = 7

_CODELISTS = {
    'arealressurs_arealtype': 'https://register.geonorge.no/api/sosi-kodelister/fkb/ar5/5.0/arealressursarealtype.json',
    'fullstendighet_dekning': 'https://register.geonorge.no/api/sosi-kodelister/temadata/fullstendighetsdekningskart/dekningsstatus.json',
    'vegkategori': 'https://register.geonorge.no/api/sosi-kodelister/kartdata/vegkategori.json'
}


async def get_codelist(type: str) -> List[Dict] | None:
    url = _CODELISTS.get(type)

    if url is None:
        return None

    file_path = Path(
        path.join(CACHE_DIR, f'codelists/{type}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, _CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        codelist = await _get_codelist(url)

        if codelist is None:
            return None

        json_object = json.dumps(codelist, indent=2)

        with file_path.open('w', encoding='utf-8') as file:
            file.write(json_object)

        return codelist
    else:
        with file_path.open(encoding='utf-8') as file:
            codelist = json.load(file)

        return codelist


async def _get_codelist(url: str) -> List[Dict]:
    response = await _fetch_codelist(url)

    if response is None:
        return None

    contained_items: List[Dict] = response.get('containeditems', [])
    entries: List[Dict] = []

    for item in contained_items:
        if item.get('status') == 'Gyldig':
            entries.append({
                'value': item.get('codevalue'),
                'label': item.get('label'),
                'description': item.get('description')
            })

    return entries


async def _fetch_codelist(url: str) -> Dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None


__all__ = ['get_codelist']