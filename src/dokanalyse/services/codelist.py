from os import path, getenv
from pathlib import Path
import json
from typing import List
import aiohttp
from ..utils.constants import APP_FILES_DIR
from ..utils.helpers.common import should_refresh_cache

__CACHE_DAYS = 7

__CODELISTS = {
    'arealressurs_arealtype': 'https://register.geonorge.no/api/sosi-kodelister/fkb/ar5/5.0/arealressursarealtype.json',
    'fullstendighet_dekning': 'https://register.geonorge.no/api/sosi-kodelister/temadata/fullstendighetsdekningskart/dekningsstatus.json',
    'vegkategori': 'https://register.geonorge.no/api/sosi-kodelister/kartdata/vegkategori.json'
}


async def get_codelist(type: str) -> List[dict] | None:
    url = __CODELISTS.get(type)

    if url is None:
        return None

    file_path = Path(
        path.join(getenv('APP_FILES_DIR'), f'resources/codelists/{type}.json'))

    if not file_path.exists() or should_refresh_cache(file_path, __CACHE_DAYS):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        codelist = await __get_codelist(url)

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


async def __get_codelist(url: str) -> List[dict]:
    response = await __fetch_codelist(url)

    if response is None:
        return None

    contained_items = response.get('containeditems', [])
    entries = []

    for item in contained_items:
        if item.get('status') == 'Gyldig':
            entries.append({
                'value': item.get('codevalue'),
                'label': item.get('label'),
                'description': item.get('description')
            })

    return entries


async def __fetch_codelist(url: str) -> dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.json()
    except:
        return None
