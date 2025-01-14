from typing import List
from .legend import create_legend


def get_wms_url(wms_url: str, wms_layers: List[str]) -> str:
    layers = ','.join(wms_layers)

    return f'{wms_url}?layers={layers}'


async def get_cartography_url(wms_url: str, wms_layers: List[str]) -> str:
    urls = []

    for wms_layer in wms_layers:
        # autopep8: off
        url = f'{wms_url}?service=WMS&version=1.3.0&request=GetLegendGraphic&sld_version=1.1.0&layer={wms_layer.strip()}&format=image/png'
        # autopep8: on
        urls.append(url)

    if len(urls) == 1:
        return urls[0]

    data_url = await create_legend(urls)

    return data_url


__all__ = ['get_wms_url', 'get_cartography_url']
