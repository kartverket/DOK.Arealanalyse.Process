import asyncio
import time
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from typing import Any, List, Dict, Tuple
from concurrent.futures import TimeoutError
import multiprocessing as mp
import structlog
from structlog.stdlib import BoundLogger
from osgeo import ogr
import pandas as pd
import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from owslib.wms import WebMapService
import cartopy.crs as ccrs
import cartopy.io.ogc_clients as ogcc
from cartopy.mpl.geoaxes import GeoAxes
from cartopy.mpl.slippy_image_artist import SlippyImageArtist
from shapely import box, Polygon
from ..models.analysis import Analysis
from ..models.fact_sheet import FactSheet
from ..models.state_emitter import StateEmitter, StateStatus
from ..utils.helpers.geometry import get_epsg_from_geometry, transform_geometry
from ..constants import CACHE_DIR

matplotlib.use('agg')

ogcc.METERS_PER_UNIT['EPSG:3857'] = 1
ogcc._URN_TO_CRS['EPSG:3857'] = ccrs.GOOGLE_MERCATOR

_WMTS_URL = 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml?request=GetCapabilities'
_TIMEOUT_SECONDS = 120
_DPI = 100

_logger: BoundLogger = structlog.get_logger(__name__)
_semaphore = asyncio.Semaphore(min(mp.cpu_count(), 8))

_basemaps_cache_dir = Path(CACHE_DIR).joinpath('basemaps')
_basemaps_cache_dir.mkdir(parents=True, exist_ok=True)


async def generate_map_images(
        analyses: List[Analysis],
        fact_sheet: FactSheet | None,
        state: StateEmitter,
) -> List[Tuple[str, str, bytes | None]]:
    start = time.time()

    params: List[Dict] = []
    params.extend(_get_params_for_analyses(analyses))

    if fact_sheet:
        params.append(_get_params_for_fact_sheet(fact_sheet))

    if not params:
        return []

    state.map_images_total = len(params)
    state.send_message(StateStatus.CREATING_MAP_IMAGES)

    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(
            _generate_map_image(**kwargs)) for kwargs in params]

    results = [task.result() for task in tasks if task]

    _logger.info('Generated map images', count=len(results),
                 duration=round(time.time() - start, 2))

    return results


async def _generate_map_image(**kwargs) -> Tuple[str, str, bytes | None] | None:
    async with _semaphore:
        try:
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                return await asyncio.to_thread(_create_map_image, **kwargs)
        except TimeoutError as err:
            _logger.error('Map image generation timed out', error=str(err))
            return None
        except Exception as err:
            _logger.error('Map image generation failed', error=str(err))
            return None


def _create_map_image(**kwargs) -> Tuple[str, str, bytes | None]:
    id: str = kwargs['id']
    name: str = kwargs['name']
    wkt_str: str = kwargs['geometry']
    grayscale: bool = kwargs.get('grayscale', False)

    _logger.info('Generating map image', config_id=id, dataset=name)

    gdf = gpd.GeoSeries.from_wkt([wkt_str])
    crs_epsg = ccrs.epsg('3857')

    size: Tuple[int, int] = kwargs.get('size', (800, 600))
    width, height = size
    figsize = _get_figsize(width, height)

    fig, ax = plt.subplots(figsize=figsize, dpi=_DPI, subplot_kw={
        'projection': crs_epsg})

    ax.axis('off')

    _add_wmts(ax, grayscale)

    gdf.plot(ax=ax, edgecolor='#d33333',
             facecolor='none', linewidth=3)

    buffer: str = kwargs.get('buffer', '')

    if buffer:
        buffer_row = gpd.GeoSeries.from_wkt([buffer])
        gdf: gpd.GeoSeries = pd.concat([gdf, buffer_row], ignore_index=True)
        gdf.plot(ax=ax, edgecolor='#d33333', facecolor='none',
                 linestyle='--', linewidth=1.5)

    ext_bbox = _extend_bbox_to_aspect_ratio(gdf.total_bounds, width/height)
    ext_bbox_row = gpd.GeoSeries.from_wkt([ext_bbox.wkt])

    gdf: gpd.GeoSeries = pd.concat([gdf, ext_bbox_row], ignore_index=True)
    gdf.plot(ax=ax, facecolor='none', linewidth=0)

    wms: Dict = kwargs.get('wms')

    if wms:
        _add_wms(ax, wms.get('url'), wms.get('layers'))

    bytes_out = _convert_to_bytes(fig)

    plt.close()

    return id, name, bytes_out


def _get_params_for_analyses(analyses: List[Analysis]) -> List[Dict[str, Any]]:
    params: List[Dict[str, Any]] = []

    for analysis in analyses:
        url, layers = _parse_wms_url(analysis.raster_result_map)
        buffered_geom = None

        if analysis.buffer > 0:
            buffered_geom: ogr.Geometry = analysis.geometry.Buffer(
                analysis.buffer)
            buffer = _get_wkt_str(buffered_geom)
        else:
            buffer = None

        params.append({
            'id': str(analysis.config_id),
            'name': analysis.config.name,
            'size': (1280, 720),
            'geometry': _get_wkt_str(analysis.geometry),
            'buffer': buffer,
            'grayscale': True,
            'wms': {
                'url': url,
                'layers': layers
            }
        })

    return params


def _get_params_for_fact_sheet(fact_sheet: FactSheet) -> Dict[str, Any]:
    buffered_geom = None

    if fact_sheet.buffer > 0:
        buffered_geom: ogr.Geometry = fact_sheet.geometry.Buffer(
            fact_sheet.buffer)
        buffer = _get_wkt_str(buffered_geom)
    else:
        buffer = None

    return {
        'id': 'omraade',
        'name': 'omraade',
        'size': (1280, 720),
        'geometry': _get_wkt_str(fact_sheet.geometry),
        'buffer': buffer,
        'grayscale': False
    }


def _add_wmts(ax: GeoAxes, grayscale: bool) -> SlippyImageArtist:
    layer_name = 'topograatone' if grayscale else 'topo'
    cache_dir = _basemaps_cache_dir / layer_name

    return ax.add_wmts(_WMTS_URL, layer_name, cache=str(cache_dir))


def _add_wms(ax: GeoAxes, url: str, layers: List[str]) -> SlippyImageArtist:
    wms = WebMapService(url, '1.3.0')

    return ax.add_wms(wms=wms, layers=layers)


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


def _convert_to_bytes(fig: Figure) -> bytes:
    fig.tight_layout(pad=0)
    fig.set_frameon(False)

    byte_stream = BytesIO()
    fig.savefig(byte_stream, format='png', dpi=100)

    return byte_stream.getvalue()


def _get_wkt_str(geometry: ogr.Geometry) -> str:
    src_epsg = get_epsg_from_geometry(geometry) or 4326
    transd_geom = transform_geometry(geometry, src_epsg, 3857)

    return transd_geom.ExportToWkt()


def _get_figsize(width: int, height: int) -> Tuple[int, int]:
    return (width / _DPI, height / _DPI)


def _parse_wms_url(url: str) -> Tuple[str, List[str]]:
    parsed = urlparse(url)
    query_strings = parse_qs(parsed.query)
    layers_list: List[str] = query_strings.get('layers', [])

    base_url = f'{parsed.scheme}://{parsed.netloc}{parsed.path}'
    layers = layers_list[0].split(',') if len(layers_list) == 1 else []

    return base_url, layers


__all__ = ['generate_map_images']
