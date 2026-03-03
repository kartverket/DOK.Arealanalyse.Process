from typing import List
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


async def get_cartography_url(wms_url: str, wms_layers: List[str], geometry: ogr.Geometry) -> str:
    dynamic_legend = await _dynamic_legend_supported(wms_url)
    urls = []

    for wms_layer in wms_layers:
        url = _create_legend_url(wms_url, wms_layer, geometry, dynamic_legend)
        urls.append(url)

    if len(urls) == 1:
        return urls[0]

    data_url = await create_legend(urls)

    return data_url


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

    query_str = '&'.join([f'{key}={value}' for key, value in params.items()])

    return f'{base_url}?{query_str}'


@alru_cache(maxsize=4096)
async def _dynamic_legend_supported(wms_url: str) -> bool:
    url = f'{wms_url}?service=WMS&version=1.3.0&request=GetCapabilities'

    try:
        async with get_session().get(url) as response:
            response.raise_for_status()
            text = await response.text()
            substr = text[:1000]

            if 'mapserver' in substr:
                return False

            return True
    except Exception as err:
        _logger.error('Reading WMS capabilities failed',
                      url=url, error=str(err))
        return False


__all__ = ['get_wms_url', 'get_cartography_url']
