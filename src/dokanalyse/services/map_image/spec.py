from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs
from typing import List, Tuple, cast
from osgeo import ogr
from shapely import from_wkt
from ...models.analysis import Analysis
from ...models.fact_sheet import FactSheet
from ...utils.helpers.geometry import get_epsg_from_geometry, transform_geometry

# minx, miny, maxx, maxy in EPSG:3857
Extent = Tuple[float, float, float, float]

_SIZE = (1280, 720)
# Space around the polygon (or buffer), as a fraction of the extent on each side
_MARGIN = 0.05


@dataclass
class MapSpec:
    """One map image. Geometries are WKT in EPSG:3857."""
    id: str
    name: str
    geometry: str
    buffer: str | None
    extent: Extent
    grayscale: bool
    wms_url: str | None = None
    wms_layers: List[str] | None = None
    size: Tuple[int, int] = _SIZE


def get_map_specs(analyses: List[Analysis], fact_sheet: FactSheet | None) -> List[MapSpec]:
    specs: List[MapSpec] = []

    for analysis in analyses:
        url, layers = _parse_wms_url(cast(str, analysis.raster_result_map))

        if not layers:
            continue

        geometry, buffer = _get_geometries(analysis.geometry, analysis.buffer)

        specs.append(MapSpec(
            id=str(analysis.config_id),
            name=analysis.config.name,
            geometry=geometry,
            buffer=buffer,
            extent=get_extent(geometry, buffer, _SIZE),
            grayscale=True,
            wms_url=url,
            wms_layers=layers
        ))

    if fact_sheet:
        geometry, buffer = _get_geometries(fact_sheet.geometry, fact_sheet.buffer)

        specs.append(MapSpec(
            id='omraade',
            name='omraade',
            geometry=geometry,
            buffer=buffer,
            extent=get_extent(geometry, buffer, _SIZE),
            grayscale=False
        ))

    return specs


def get_extent(geometry: str, buffer: str | None, size: Tuple[int, int]) -> Extent:
    """The bounds of the buffer (or the geometry, without a buffer), extended
    to the aspect ratio of the image, with a margin on each side."""
    minx, miny, maxx, maxy = from_wkt(buffer or geometry).bounds
    width = maxx - minx
    height = maxy - miny
    target_ratio = size[0] / size[1]

    if width / height > target_ratio:
        height = width / target_ratio
    else:
        width = height * target_ratio

    center_x = (minx + maxx) / 2
    center_y = (miny + maxy) / 2
    half_width = width * (0.5 + _MARGIN)
    half_height = height * (0.5 + _MARGIN)

    return center_x - half_width, center_y - half_height, center_x + half_width, center_y + half_height


def _get_geometries(geometry: ogr.Geometry, buffer: int) -> Tuple[str, str | None]:
    buffered = _get_wkt_str(geometry.Buffer(buffer)) if buffer > 0 else None

    return _get_wkt_str(geometry), buffered


def _get_wkt_str(geometry: ogr.Geometry) -> str:
    src_epsg = get_epsg_from_geometry(geometry) or 4326
    transd_geom = transform_geometry(geometry, src_epsg, 3857)

    return transd_geom.ExportToWkt()


def _parse_wms_url(url: str) -> Tuple[str, List[str]]:
    parsed = urlparse(url)
    query_strings = parse_qs(parsed.query)
    layers_list: List[str] = query_strings.get('layers', [])

    base_url = f'{parsed.scheme}://{parsed.netloc}{parsed.path}'
    layers = layers_list[0].split(',') if len(layers_list) == 1 else []

    return base_url, layers


__all__ = ['MapSpec', 'Extent', 'get_map_specs', 'get_extent']
