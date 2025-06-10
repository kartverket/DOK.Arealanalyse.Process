import logging
import time
from io import BytesIO
from uuid import UUID
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Tuple
from osgeo import ogr, osr
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.image import AxesImage
import cartopy.crs as ccrs
import cartopy.io.ogc_clients as ogcc
from cartopy.mpl.geoaxes import GeoAxes
import contextily as ctx
from shapely import box, Polygon
from ..models.analysis import Analysis
from ..models.fact_sheet import FactSheet

ogcc.METERS_PER_UNIT['EPSG:3857'] = 1
ogcc._URN_TO_CRS['EPSG:3857'] = ccrs.GOOGLE_MERCATOR

_LOGGER = logging.getLogger(__name__)
_WMTS_URL = 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml?request=GetCapabilities'
_DPI = 100


def generate_map_images(analyses: List[Analysis], fact_sheet: FactSheet | None) -> List[Tuple[UUID, bytes | None]]:
    params: List[Dict] = []

    params.extend(_get_params_for_analyses(analyses))

    if fact_sheet:
        params.append(_get_params_for_fact_sheet(fact_sheet))

    with ProcessPoolExecutor() as executor:
        start = time.time()

        futures = [executor.submit(_generate_map_image, **kwargs)
                   for kwargs in params]
        results = [future.result() for future in futures]

        # autopep8: off
        _LOGGER.info(f'Generated {len(futures)} map images in {round(time.time() - start, 2)} sec.')
        # autopep8: on

        return results


def _generate_map_image(**kwargs) -> Tuple[str, bytes | None]:
    id: UUID = kwargs['id']
    wkt_str: str = kwargs['geometry']
    gdf = gpd.GeoSeries.from_wkt([wkt_str])
    crs_epsg = ccrs.epsg('3857')

    size: Tuple[int, int] = kwargs.get('size', (800, 600))
    width, height = size
    figsize = _get_figsize(width, height)

    fig, ax = plt.subplots(figsize=figsize, dpi=_DPI, subplot_kw={
        'projection': crs_epsg})

    ax.axis('off')

    gdf.plot(ax=ax, edgecolor='#d33333',
             facecolor='none', linewidth=3)

    buffer: int = kwargs.get('buffer', 0)

    if buffer > 0:
        gdf = gdf.buffer(buffer)
        gdf.plot(ax=ax, edgecolor='#d33333', facecolor='none',
                 linestyle='--', linewidth=1.5)

    ext_bbox = _extend_bbox_to_aspect_ratio(gdf.total_bounds, width/height)
    ext_bbox_row = gpd.GeoSeries.from_wkt([ext_bbox.wkt])

    gdf: gpd.GeoSeries = pd.concat([gdf, ext_bbox_row], ignore_index=True)
    gdf.plot(ax=ax, facecolor='none', linewidth=0)

    grayscale: bool = kwargs.get('grayscale', False)
    use_wmts: bool = kwargs.get('use_wmts', False)

    try:
        _add_basemap(ax, use_wmts, grayscale)
    except:
        return str(id), None

    wms: Dict = kwargs.get('wms')

    if wms:
        try:
            _add_wms(ax, wms.get('url'), wms.get('layers'))
        except:
            return str(id), None

    image_bytes = _convert_to_bytes(fig)

    plt.close()

    return str(id), image_bytes


def _get_params_for_analyses(analyses: List[Analysis]) -> List[Dict]:
    params: List[Dict] = []

    for analysis in analyses:
        url, layers = _parse_wms_url(analysis.raster_result_map)

        params.append({
            'id': analysis.config_id,
            'size': (1280, 720),
            'geometry': _get_wkt_str(analysis.geometry),
            'buffer': analysis.buffer,
            'grayscale': True,
            'use_wmts': True,
            'wms': {
                'url': url,
                'layers': layers
            }
        })

    return params


def _get_params_for_fact_sheet(fact_sheet: FactSheet) -> Dict:
    return {
        'id': 'fact_sheet',
        'size': (1280, 548),
        'geometry': _get_wkt_str(fact_sheet.geometry),
        'buffer': fact_sheet.buffer,
        'grayscale': False,
        'use_wmts': True
    }


def _add_basemap(ax: GeoAxes, use_wmts: bool, grayscale: bool) -> None:
    if use_wmts:
        layer_name = 'topograatone' if grayscale else 'topo'
        ax.add_wmts(_WMTS_URL, layer_name=layer_name)
    else:
        basemap_src = ctx.providers.CartoDB.Positron if grayscale else ctx.providers.OpenStreetMap.Mapnik
        ctx.add_basemap(ax, source=basemap_src)


def _add_wms(ax: GeoAxes, url: str, layers: List[str]) -> AxesImage:
    return ax.add_wms(wms=url, layers=layers)


def _extend_bbox_to_aspect_ratio(bounds: Tuple[float, float, float, float], target_aspect_ratio: float) -> Polygon:
    minx, miny, maxx, maxy = bounds
    width = maxx - minx
    height = maxy - miny
    center_x = (minx + maxx) / 2
    center_y = (miny + maxy) / 2

    current_aspect = width / height

    if current_aspect > target_aspect_ratio:
        new_height = width / target_aspect_ratio
        new_width = width
    else:
        new_width = height * target_aspect_ratio
        new_height = height

    half_width = new_width / 2
    half_height = new_height / 2

    new_minx = center_x - half_width
    new_maxx = center_x + half_width
    new_miny = center_y - half_height
    new_maxy = center_y + half_height

    return box(new_minx, new_miny, new_maxx, new_maxy)


def _convert_to_bytes(fig: Figure, width: int, height: int) -> bytes:
    fig.tight_layout(pad=0)
    fig.set_frameon(False)

    byte_arr = BytesIO()
    fig.savefig(byte_arr, format='png', dpi=100)

    return byte_arr.getvalue()


def _get_wkt_str(geometry: ogr.Geometry) -> str:
    src_epsg = _get_epsg_code(geometry)
    coord_trans = _get_coordinate_transformation(src_epsg, 3857)
    geometry.Transform(coord_trans)

    return geometry.ExportToWkt()


def _get_epsg_code(geometry: ogr.Geometry) -> int:
    sr: osr.SpatialReference = geometry.GetSpatialReference()
    epsg_str: str = sr.GetAuthorityCode(None)

    return int(epsg_str)


def _get_coordinate_transformation(src_epsg: int, target_epsg: int) -> osr.CoordinateTransformation:
    source: osr.SpatialReference = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)

    target: osr.SpatialReference = osr.SpatialReference()
    target.ImportFromEPSG(target_epsg)

    return osr.CoordinateTransformation(source, target)


def _get_figsize(width: int, height: int) -> Tuple[int, int]:
    return (width / _DPI, height / _DPI)


def _parse_wms_url(url: str) -> Tuple[str, List[str]]:
    parsed = urlparse(url)
    query_strings = parse_qs(parsed.query)
    layers_list: List[str] = query_strings.get('layers', [])

    base_url = f'{parsed.scheme}://{parsed.netloc}{parsed.path}'
    layers = layers_list[0].split(',') if len(layers_list) == 1 else []

    return base_url, layers
