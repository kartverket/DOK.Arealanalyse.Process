import asyncio
import time
from typing import List, Tuple
import structlog
from structlog.stdlib import BoundLogger
from PIL import Image
from .basemap import fetch_basemap
from .render import render_map_image
from .spec import MapSpec, get_map_specs
from .wms import fetch_wms_image
from ...models.analysis import Analysis
from ...models.fact_sheet import FactSheet
from ...models.progress_emitter import ProgressEmitter, ProgressStatus
from ...utils.async_executor import run_in_render_thread

# Map images are drawn in EPSG:3857. The basemap tiles and the WMS images are
# fetched concurrently on the event loop, in EPSG:3857 and for the exact
# extent of the image, so nothing has to be reprojected. Only the drawing
# itself runs in the render thread.

_FETCH_TIMEOUT_SECONDS = 120

_logger: BoundLogger = structlog.get_logger(__name__)


async def generate_map_images(
        analyses: List[Analysis],
        fact_sheet: FactSheet | None,
        state: ProgressEmitter,
) -> List[Tuple[str, str, bytes | None]]:
    start = time.time()
    specs = get_map_specs(analyses, fact_sheet)

    if not specs:
        return []

    state.map_images_total = len(specs)

    async def generate(spec: MapSpec) -> Tuple[str, str, bytes | None] | None:
        result = await _generate_map_image(spec)
        state.send_message(ProgressStatus.CREATING_MAP_IMAGES)

        return result

    generated = await asyncio.gather(*(generate(spec) for spec in specs))
    results = [result for result in generated if result]

    _logger.info('Generated map images', count=len(results),
                 duration=round(time.time() - start, 2))

    return results


async def _generate_map_image(spec: MapSpec) -> Tuple[str, str, bytes | None] | None:
    start = time.time()

    try:
        async with asyncio.timeout(_FETCH_TIMEOUT_SECONDS):
            basemap, wms_image = await asyncio.gather(
                fetch_basemap('topograatone' if spec.grayscale else 'topo', spec.extent, spec.size),
                _fetch_wms_image(spec))

        fetched = time.time()
        image = await run_in_render_thread(render_map_image, spec, basemap, wms_image)
    except TimeoutError:
        _logger.error('Map image generation timed out', config_id=spec.id, dataset=spec.name)
        return None
    except Exception as err:
        _logger.error('Map image generation failed',
                      config_id=spec.id, dataset=spec.name, error=str(err) or type(err).__name__)
        return None

    _logger.info('Generated map image', config_id=spec.id, dataset=spec.name,
                 tiles=basemap.tiles, tiles_downloaded=basemap.downloaded,
                 fetch_duration=round(fetched - start, 2),
                 render_duration=round(time.time() - fetched, 2))

    return spec.id, spec.name, image


async def _fetch_wms_image(spec: MapSpec) -> Image.Image | None:
    if not spec.wms_url or not spec.wms_layers:
        return None

    return await fetch_wms_image(spec.wms_url, spec.wms_layers, spec.extent, spec.size)


__all__ = ['generate_map_images']
