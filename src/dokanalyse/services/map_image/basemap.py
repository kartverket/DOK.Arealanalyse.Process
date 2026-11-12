import asyncio
import functools
import os
import tempfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Tuple, TypeVar
from owslib.wmts import WebMapTileService, TileMatrix, TileMatrixSet
from PIL import Image
from .spec import Extent
from .wms import ensure_image
from ...caching.wmts import get_or_create_wmts_capabilities, WMTS_CAPABILITIES_URL
from ...utils.http_context import get_session
from ...constants import CACHE_DIR

# The basemap is a mosaic of WMTS tiles in EPSG:3857 (the tile matrix set
# "webmercator" in Kartverket's WMTS), so it never has to be reprojected.
# Tiles are cached on disk and never expire.

# Standard pixel size (0.28 mm) used by WMTS scale denominators
_METERS_PER_PIXEL = 0.28e-3
# Tiles that only touch the extent (one part in a million) are not fetched
_EPSILON = 1e-6

_tiles_cache_dir = Path(CACHE_DIR).joinpath('basemaps')

# The parsed capabilities and the modification time of the cached file
_wmts_service: Tuple[float, WebMapTileService] | None = None
# Tile downloads in progress, shared by all map images and requests, so a
# tile is never downloaded twice at the same time
_in_flight: Dict[Path, asyncio.Task] = {}

T = TypeVar('T')


@dataclass
class Basemap:
    image: Image.Image
    extent: Extent
    tiles: int
    downloaded: int


async def fetch_basemap(layer_name: str, extent: Extent, size: Tuple[int, int]) -> Basemap:
    """Returns the tiles of the layer covering the extent, at a resolution of
    at least `size` pixels, as one image."""
    wmts = await _get_wmts_service()
    matrix_set = _get_web_mercator_matrix_set(wmts, layer_name)
    matrix = _choose_tile_matrix(matrix_set.tilematrix.values(), (extent[2] - extent[0]) / size[0])
    limits = wmts.contents[layer_name].tilematrixsetlinks[matrix_set.identifier] \
        .tilematrixlimits.get(matrix.identifier)
    min_col, max_col, min_row, max_row = _select_tiles(matrix, limits, extent)

    cache_dir = _tiles_cache_dir.joinpath(layer_name, matrix_set.identifier, str(matrix.identifier))
    tiles: List[Tuple[Path, int, int]] = []
    downloads: List[Awaitable[None]] = []

    for row in range(min_row, max_row + 1):
        for col in range(min_col, max_col + 1):
            path = cache_dir.joinpath(str(row), f'{col}.png')
            left = (col - min_col) * matrix.tilewidth
            top = (row - min_row) * matrix.tileheight
            tiles.append((path, left, top))

            if not path.exists():
                url = _get_tile_url(wmts, layer_name, matrix_set.identifier, matrix.identifier, row, col)
                downloads.append(_run_once(path, functools.partial(_download_tile, url, path)))

    await asyncio.gather(*downloads)

    n_cols = max_col - min_col + 1
    n_rows = max_row - min_row + 1
    size_px = (n_cols * matrix.tilewidth, n_rows * matrix.tileheight)
    image = await asyncio.to_thread(_create_mosaic, tiles, size_px)

    span_x, span_y = _get_tile_span(matrix)
    matrix_minx, matrix_maxy = matrix.topleftcorner
    minx = matrix_minx + min_col * span_x
    maxy = matrix_maxy - min_row * span_y

    return Basemap(
        image=image,
        extent=(minx, maxy - n_rows * span_y, minx + n_cols * span_x, maxy),
        tiles=len(tiles),
        downloaded=len(downloads)
    )


async def _get_wmts_service() -> WebMapTileService:
    global _wmts_service

    path = await get_or_create_wmts_capabilities(get_session())
    modified = path.stat().st_mtime

    if _wmts_service is None or _wmts_service[0] != modified:
        _wmts_service = (modified, await asyncio.to_thread(_parse_capabilities, path))

    return _wmts_service[1]


def _parse_capabilities(path: Path) -> WebMapTileService:
    return WebMapTileService(WMTS_CAPABILITIES_URL, xml=path.read_bytes())


def _get_web_mercator_matrix_set(wmts: WebMapTileService, layer_name: str) -> TileMatrixSet:
    for name in wmts.contents[layer_name].tilematrixsetlinks:
        matrix_set: TileMatrixSet = wmts.tilematrixsets[name]

        if matrix_set.crs.split(':')[-1] in ('3857', '900913'):
            return matrix_set

    raise ValueError(f'No tile matrix set in EPSG:3857 for layer {layer_name}')


def _choose_tile_matrix(matrices: Iterable[TileMatrix], pixel_span: float) -> TileMatrix:
    """Returns the tile matrix with the lowest resolution that is at least as
    fine as `pixel_span` (meters per pixel), or the finest one available."""
    ordered = sorted(matrices, key=lambda matrix: matrix.scaledenominator, reverse=True)

    for matrix in ordered:
        if matrix.scaledenominator * _METERS_PER_PIXEL <= pixel_span:
            return matrix

    return ordered[-1]


def _get_tile_span(matrix: TileMatrix) -> Tuple[float, float]:
    pixel_span = matrix.scaledenominator * _METERS_PER_PIXEL

    return matrix.tilewidth * pixel_span, matrix.tileheight * pixel_span


def _select_tiles(matrix: TileMatrix, limits: Any, extent: Extent) -> Tuple[int, int, int, int]:
    """Returns min_col, max_col, min_row, max_row of the tiles covering the
    extent (WMTS 1.0.0, annex H)."""
    minx, miny, maxx, maxy = extent
    span_x, span_y = _get_tile_span(matrix)
    matrix_minx, matrix_maxy = matrix.topleftcorner

    min_col = max(int((minx - matrix_minx) / span_x + _EPSILON), 0)
    max_col = min(int((maxx - matrix_minx) / span_x - _EPSILON), matrix.matrixwidth - 1)
    min_row = max(int((matrix_maxy - maxy) / span_y + _EPSILON), 0)
    max_row = min(int((matrix_maxy - miny) / span_y - _EPSILON), matrix.matrixheight - 1)

    if limits:
        min_col = max(min_col, limits.mintilecol)
        max_col = min(max_col, limits.maxtilecol)
        min_row = max(min_row, limits.mintilerow)
        max_row = min(max_row, limits.maxtilerow)

    return min_col, max_col, min_row, max_row


def _get_tile_url(wmts: WebMapTileService, layer_name: str, matrix_set: str, matrix: str, row: int, col: int) -> str:
    # Kartverket's WMTS only supports RESTful requests (ResourceURL templates)
    url = wmts.buildTileResource(
        layer=layer_name, tilematrixset=matrix_set, tilematrix=matrix, row=row, column=col)

    if not url:
        raise ValueError(f'No ResourceURL template for tiles in layer {layer_name}')

    return url


async def _download_tile(url: str, path: Path) -> None:
    # Another request may have downloaded it after the caller checked
    if path.exists():
        return

    async with get_session().get(url) as response:
        await ensure_image(response)
        data = await response.read()

    await asyncio.to_thread(_write_tile, path, data)


def _write_tile(path: Path, data: bytes) -> None:
    # A broken tile in the cache would break every map image that uses it
    with Image.open(BytesIO(data)) as image:
        image.verify()

    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(dir=path.parent, suffix='.tmp', delete=False) as file:
        file.write(data)

    os.replace(file.name, path)


def _create_mosaic(tiles: List[Tuple[Path, int, int]], size: Tuple[int, int]) -> Image.Image:
    mosaic = Image.new('RGBA', size, (255, 255, 255, 255))

    for path, left, top in tiles:
        with Image.open(path) as tile:
            mosaic.paste(tile.convert('RGBA'), (left, top))

    return mosaic


async def _run_once(key: Path, factory: Callable[[], Awaitable[T]]) -> T:
    task = _in_flight.get(key)

    if task is None:
        task = asyncio.ensure_future(factory())
        _in_flight[key] = task
        task.add_done_callback(functools.partial(_on_in_flight_done, key))

    # A caller that times out must not cancel the download for the others
    return await asyncio.shield(task)


def _on_in_flight_done(key: Path, task: asyncio.Task) -> None:
    _in_flight.pop(key, None)

    # Retrieve the exception, so it is not reported as never retrieved when
    # all callers have given up
    if not task.cancelled():
        task.exception()


__all__ = ['Basemap', 'fetch_basemap']
