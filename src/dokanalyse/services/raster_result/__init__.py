from typing import List
from osgeo import ogr
from cachetools import cached, LRUCache, keys
import requests
from .legend import create_legend

_sessions_cache = LRUCache(maxsize=1000)


def get_wms_url(wms_url: str, wms_layers: List[str]) -> str:
    layers = ','.join(wms_layers)

    return f'{wms_url}?layers={layers}'


async def get_cartography_url(wms_url: str, wms_layers: List[str], geometry: ogr.Geometry) -> str:
    dynamic_legend = _dynamic_legend_supported(wms_url)
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
        params['crs'] ='EPSG:25833'
        params['legend_options'] = 'countMatched:true;fontAntiAliasing:true;hideEmptyRules:true'        

    query_str = '&'.join([f'{key}={value}' for key, value in params.items()])

    return f'{base_url}?{query_str}'


@cached(cache=_sessions_cache, key=lambda wms_url: keys.hashkey(wms_url))
def _dynamic_legend_supported(wms_url: str) -> bool:
    url = f'{wms_url}?service=WMS&version=1.3.0&request=GetCapabilities'

    try:
        response = requests.get(url)

        if response.status_code != 200:
            return False

        substr = response.text[:1000]

        if 'mapserver' in substr:
            return False

        return True
    except:
        return False


__all__ = ['get_wms_url', 'get_cartography_url']
