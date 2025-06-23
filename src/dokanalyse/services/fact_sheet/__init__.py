from typing import List
from asyncio import Task, TaskGroup
from osgeo import ogr
from .area_types import get_area_types
from .buildings import get_buildings
from .roads import get_roads
from ...utils.helpers.geometry import create_buffered_geometry
from ...utils.constants import DEFAULT_EPSG
from ...models.fact_sheet import FactSheet


async def create_fact_sheet(geometry: ogr.Geometry, orig_epsg: int, buffer: int) -> FactSheet:
    fact_sheet = FactSheet()
    fact_sheet.geometry = geometry
    fact_sheet.buffer = buffer

    tasks = await _run_tasks(geometry, orig_epsg, buffer)

    for task in tasks:
        if task.get_name() == 'raster_result':
            fact_sheet.raster_result_image_bytes = task.result()
        else:
            result = task.result()

            if result:
                fact_sheet.fact_list.append(result)

    return fact_sheet


async def _run_tasks(geometry: ogr.Geometry, orig_epsg: int, buffer: int) -> List[Task]:
    input_geom = create_buffered_geometry(geometry, buffer, DEFAULT_EPSG)
    tasks: List[Task]

    async with TaskGroup() as tg:
        tasks = [
            tg.create_task(get_area_types(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer)),
            tg.create_task(get_buildings(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer)),
            # tg.create_task(get_roads(
            #     input_geom, DEFAULT_EPSG, orig_epsg, buffer)),
        ]

    return tasks


__all__ = ['create_fact_sheet']
