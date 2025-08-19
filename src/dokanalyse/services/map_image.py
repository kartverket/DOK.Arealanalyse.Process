import logging
import time
import os
import hashlib
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from typing import List, Dict, Tuple
from pebble import ProcessPool
from concurrent.futures import TimeoutError, as_completed
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
from PIL import Image
from PIL.ImageFile import ImageFile
from ..models.analysis import Analysis
from ..models.fact_sheet import FactSheet
from ..utils.helpers.common import should_refresh_cache
from ..utils.constants import CACHE_DIR

ogcc.METERS_PER_UNIT['EPSG:3857'] = 1
ogcc._URN_TO_CRS['EPSG:3857'] = ccrs.GOOGLE_MERCATOR

_LOGGER = logging.getLogger(__name__)
_WMTS_URL = 'https://cache.kartverket.no/v1/wmts/1.0.0/WMTSCapabilities.xml?request=GetCapabilities'
_DPI = 100
_TIMEOUT_SECONDS = 120
_BASEMAPS_CACHE_DIR = f'{CACHE_DIR}/basemaps'
_BASEMAPS_CACHE_DAYS = 30

if not os.path.exists(_BASEMAPS_CACHE_DIR):
    os.makedirs(_BASEMAPS_CACHE_DIR)


def generate_map_images(analyses: List[Analysis], fact_sheet: FactSheet | None) -> List[Tuple[str, str, bytes | None]]:
    start = time.time()

    params: List[Dict] = []
    params.extend(_get_params_for_analyses(analyses))

    if fact_sheet:
        params.append(_get_params_for_fact_sheet(fact_sheet))

    _generate_basemaps(params)

    results: List[Tuple[str, bytes | None]] = []

    with ProcessPool(max_workers=8) as pool:
        futures = [pool.submit(
            _generate_map_image, _TIMEOUT_SECONDS, **kwargs) for kwargs in params]

        for future in as_completed(futures):
            try:
                results.append(future.result())
            except TimeoutError as error:
                _LOGGER.error('Map image generation timed out')
            except Exception as error:
                _LOGGER.error('Map image generation failed: %s' % error)

    # autopep8: off
    _LOGGER.info(f'Generated {len(results)} map images in {round(time.time() - start, 2)} sec.')
    # autopep8: on

    return results


def _generate_basemaps(params: List[Dict]) -> None:
    basemap_params: Dict[str, Dict] = {}

    for params_dict in params:
        hash_str = params_dict['hash']

        if not hash_str in basemap_params and not _get_cached_basemap_path(hash_str):
            basemap_params[hash_str] = params_dict

    if not basemap_params:
        return

    results: List[Tuple[str, bytes | None]] = []

    with ProcessPool(max_workers=8) as pool:
        futures = [pool.submit(
            _generate_basemap, _TIMEOUT_SECONDS, **kwargs) for kwargs in basemap_params.values()]

        for future in as_completed(futures):
            try:
                results.append(future.result())
            except TimeoutError as error:
                _LOGGER.error('Basemap generation timed out')
            except Exception as error:
                _LOGGER.error('Basemap generation failed: %s' % error)

    for hash_str, img_bytes in results:
        if not img_bytes:
            continue

        filename = f'{_BASEMAPS_CACHE_DIR}/{hash_str}.png'

        with open(filename, 'wb') as file:
            file.write(img_bytes)


def _generate_map_image(**kwargs) -> Tuple[str, str, bytes | None]:
    id: str = kwargs['id']
    name: str = kwargs['name']
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

    buffer: str = kwargs.get('buffer')

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
        try:
            _add_wms(ax, wms.get('url'), wms.get('layers'))
        except:
            return id, name, None

    fig_image_bytes = _convert_to_bytes(fig)
    base_img = _get_cached_basemap(kwargs['hash'])
    byte_stream = BytesIO()

    if base_img:
        fig_img = Image.open(BytesIO(fig_image_bytes))
        base_img.paste(fig_img, (0, 0), fig_img)
        base_img.save(byte_stream, format='png')
        bytes_out = byte_stream.getvalue()
    else:
        bytes_out = fig_image_bytes

    plt.close()

    return id, name, bytes_out


def _generate_basemap(**kwargs) -> Tuple[str, bytes | None]:
    geometry: str = kwargs['geometry']
    buffer: str = kwargs.get('buffer')
    wkt_str = buffer if buffer else geometry

    gdf = gpd.GeoSeries.from_wkt([wkt_str])
    crs_epsg = ccrs.epsg('3857')

    size: Tuple[int, int] = kwargs.get('size', (800, 600))
    width, height = size
    figsize = _get_figsize(width, height)

    fig, ax = plt.subplots(figsize=figsize, dpi=_DPI, subplot_kw={
        'projection': crs_epsg})

    ax.axis('off')

    gdf.plot(ax=ax, facecolor='none', linewidth=0)

    ext_bbox = _extend_bbox_to_aspect_ratio(gdf.total_bounds, width/height)
    ext_bbox_row = gpd.GeoSeries.from_wkt([ext_bbox.wkt])

    gdf: gpd.GeoSeries = pd.concat([gdf, ext_bbox_row], ignore_index=True)
    gdf.plot(ax=ax, facecolor='none', linewidth=0)

    grayscale: bool = kwargs.get('grayscale', False)
    use_wmts: bool = kwargs.get('use_wmts', True)

    try:
        _add_basemap(ax, use_wmts, grayscale)
    except Exception as err:
        _LOGGER.error(err)
        return None

    image_bytes = _convert_to_bytes(fig)

    plt.close()

    return kwargs['hash'], image_bytes


def _get_params_for_analyses(analyses: List[Analysis]) -> List[Dict]:
    params: List[Dict] = []

    for analysis in analyses:
        url, layers = _parse_wms_url(analysis.raster_result_map)
        buffered_geom = None

        if analysis.buffer > 0:
            buffered_geom: ogr.Geometry = analysis.geometry.Buffer(
                analysis.buffer)
            buffer = _get_wkt_str(buffered_geom)
        else:
            buffer = None

        basemap_geom = buffered_geom if buffered_geom else analysis.geometry
        hash_str = _create_basemap_hash(basemap_geom, True)

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
            },
            'hash': hash_str
        })

    return params


def _get_params_for_fact_sheet(fact_sheet: FactSheet) -> Dict:
    buffered_geom = None

    if fact_sheet.buffer > 0:
        buffered_geom: ogr.Geometry = fact_sheet.geometry.Buffer(
            fact_sheet.buffer)
        buffer = _get_wkt_str(buffered_geom)
    else:
        buffer = None

    basemap_geom = buffered_geom if buffered_geom else fact_sheet.geometry
    hash_str = _create_basemap_hash(basemap_geom, False)

    return {
        'id': 'omraade',
        'name': 'omraade',
        'size': (1280, 720),
        'geometry': _get_wkt_str(fact_sheet.geometry),
        'buffer': buffer,
        'grayscale': False,
        'hash': hash_str
    }


def _add_basemap(ax: GeoAxes, use_wmts: bool, grayscale: bool) -> None:
    if use_wmts:
        layer_name = 'topograatone' if grayscale else 'topo'
        ax.add_wmts(_WMTS_URL, layer_name)
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


def _convert_to_bytes(fig: Figure) -> bytes:
    fig.tight_layout(pad=0)
    fig.set_frameon(False)

    byte_stream = BytesIO()
    fig.savefig(byte_stream, format='png', dpi=100)

    return byte_stream.getvalue()


def _get_wkt_str(geometry: ogr.Geometry) -> str:
    src_epsg = _get_epsg_code(geometry)
    coord_trans = _get_coordinate_transformation(src_epsg, 3857)
    clone: ogr.Geometry = geometry.Clone()

    clone.Transform(coord_trans)

    return clone.ExportToWkt()


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


def _get_cached_basemap(hash_str: str) -> ImageFile:
    path = _get_cached_basemap_path(hash_str)

    return Image.open(path) if path else None


def _get_cached_basemap_path(hash_str) -> str:
    path = Path(f'{_BASEMAPS_CACHE_DIR}/{hash_str}.png')
    absolute_path = path.resolve()

    if path.exists() and not should_refresh_cache(absolute_path, _BASEMAPS_CACHE_DAYS):
        return absolute_path

    return None


def _create_basemap_hash(geometry: ogr.Geometry, grayscale: bool) -> str:
    envelope = geometry.GetEnvelope()
    input_string = str(envelope)

    if grayscale:
        input_string += ',bw'

    encoded_string = input_string.encode('utf-8')
    sha256_hash = hashlib.sha256()
    sha256_hash.update(encoded_string)

    return sha256_hash.hexdigest()


__all__ = ['generate_map_images']
