from typing import Awaitable, List
from asyncio import Task, TaskGroup
import structlog
from structlog.stdlib import BoundLogger
from osgeo import ogr
from .area_types import get_area_types
from .buildings import get_buildings
from ...models.fact_sheet import FactSheet
from ...models.fact_part import FactPart
from ...utils.helpers.geometry import create_buffered_geometry
from ...constants import DEFAULT_EPSG

_logger: BoundLogger = structlog.get_logger(__name__)


async def create_fact_sheet(geometry: ogr.Geometry, orig_epsg: int | None, buffer: int) -> FactSheet:
    fact_sheet = FactSheet()
    fact_sheet.geometry = geometry
    fact_sheet.buffer = buffer

    tasks = await _run_tasks(geometry, orig_epsg, buffer)

    for task in tasks:
        result = task.result()

        if result:
            fact_sheet.fact_list.append(result)

    return fact_sheet


async def _run_tasks(geometry: ogr.Geometry, orig_epsg: int | None, buffer: int) -> List[Task]:
    input_geom = create_buffered_geometry(geometry, buffer, DEFAULT_EPSG)
    tasks: List[Task]

    async with TaskGroup() as tg:
        tasks = [
            tg.create_task(_run_safely('area types', get_area_types(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer))),
            tg.create_task(_run_safely('buildings', get_buildings(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer)))
        ]

    return tasks


async def _run_safely(name: str, coro: Awaitable[FactPart | None]) -> FactPart | None:
    try:
        return await coro
    except Exception as err:
        _logger.error('Fact sheet part failed', part=name, error=str(err))
        return None


__all__ = ['create_fact_sheet']
