from typing import List
from urllib.parse import urlencode
from osgeo import ogr
from async_lru import alru_cache
import structlog
from structlog.stdlib import BoundLogger
from .legend import create_legend
from ...utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


def get_wms_url(wms_url: str, wms_layers: List[str]) -> str:
    layers = ','.join(wms_layers)

    return f'{wms_url}?layers={layers}'


async def get_cartography_url(wms_url: str, wms_layers: List[str], geometry: ogr.Geometry) -> str | None:
    dynamic_legend = await _dynamic_legend_supported(wms_url)
    urls = []

    for wms_layer in wms_layers:
        url = _create_legend_url(wms_url, wms_layer, geometry, dynamic_legend)
        urls.append(url)

    if not urls:
        return None

    if len(urls) == 1:
        return urls[0]

    return await create_legend(urls)


def _create_legend_url(base_url: str, layer: str, geometry: ogr.Geometry, dynamic_legend: bool) -> str:
    params = {
        'service': 'WMS',
        'version': '1.3.0',
        'request': 'GetLegendGraphic',
        'layer': layer,
        'sld_version': '1.1.0',
        'format': 'image/png',
    }

    if dynamic_legend:
        minx, maxx, miny, maxy = geometry.GetEnvelope()
        params['bbox'] = f'{minx},{miny},{maxx},{maxy}'
        params['crs'] = 'EPSG:25833'
        params['legend_options'] = 'countMatched:true;fontAntiAliasing:true;hideEmptyRules:true'

    separator = '&' if '?' in base_url else '?'

    return f'{base_url}{separator}{urlencode(params, safe=",:;/")}'


@alru_cache(maxsize=4096, ttl=3600)
async def _dynamic_legend_supported(wms_url: str) -> bool:
    separator = '&' if '?' in wms_url else '?'
    url = f'{wms_url}{separator}service=WMS&version=1.3.0&request=GetCapabilities'

    try:
        async with get_session().get(url) as response:
            response.raise_for_status()
            # Only the start of the (potentially large) document is needed
            head = b''

            while len(head) < 1000:
                chunk = await response.content.read(1000 - len(head))

                if not chunk:
                    break

                head += chunk

            return 'mapserver' not in head.decode('utf-8', errors='ignore')
    except Exception as err:
        _logger.error('Reading WMS capabilities failed',
                      url=url, error=str(err))
        return False


__all__ = ['get_wms_url', 'get_cartography_url']
