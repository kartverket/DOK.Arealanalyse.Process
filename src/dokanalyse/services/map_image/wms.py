import asyncio
from io import BytesIO
from typing import List, Tuple
import aiohttp
from lxml import etree as ET
from PIL import Image
from .spec import Extent
from ...utils.http_context import get_session

# Error texts from the services are included in the error message, up to this length
_MAX_ERROR_LENGTH = 300


async def fetch_wms_image(url: str, layers: List[str], extent: Extent, size: Tuple[int, int]) -> Image.Image:
    """Returns the WMS layers for the extent, in EPSG:3857 and exactly `size`
    pixels, so the image can be drawn on the map without reprojection."""
    minx, miny, maxx, maxy = extent
    width, height = size

    params = {
        'SERVICE': 'WMS',
        'VERSION': '1.3.0',
        'REQUEST': 'GetMap',
        'LAYERS': ','.join(layers),
        'STYLES': '',
        'CRS': 'EPSG:3857',
        'BBOX': f'{minx},{miny},{maxx},{maxy}',
        'WIDTH': str(width),
        'HEIGHT': str(height),
        'FORMAT': 'image/png',
        'TRANSPARENT': 'TRUE'
    }

    async with get_session().get(url, params=params) as response:
        await ensure_image(response)
        data = await response.read()

    return await asyncio.to_thread(_decode_image, data)


async def ensure_image(response: aiohttp.ClientResponse) -> None:
    """Raises an error with the reason given by the service, if the response
    is not an image. WMS and WMTS services report errors (an unknown layer,
    an unsupported CRS) as XML, often with status 200.

    The request URL is left out of the error on purpose: it contains the
    bounding box of the input polygon.
    """
    if response.ok and response.content_type.startswith('image/'):
        return

    reason = _get_error_text(await response.read())

    raise ValueError(f'Expected an image, got HTTP {response.status} ({response.content_type}): {reason}')


def _get_error_text(body: bytes) -> str:
    texts: List[str] = []

    try:
        parser = ET.XMLParser(resolve_entities=False, no_network=True)
        root = ET.fromstring(body, parser)
        texts = [elem.text for elem in root.iter('{*}ServiceException', '{*}ExceptionText') if elem.text]
    except ET.XMLSyntaxError:
        pass

    text = ' '.join(texts) if texts else body.decode(errors='replace')

    return ' '.join(text.split())[:_MAX_ERROR_LENGTH]


def _decode_image(data: bytes) -> Image.Image:
    with Image.open(BytesIO(data)) as image:
        return image.convert('RGBA')


__all__ = ['fetch_wms_image', 'ensure_image']
