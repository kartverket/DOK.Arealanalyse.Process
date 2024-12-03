from typing import List
import asyncio
from osgeo import ogr
from .area_types import get_area_types
from .buildings import get_buildings
from .roads import get_roads
from ...utils.helpers.geometry import create_buffered_geometry
from ...utils.constants import DEFAULT_EPSG
from ...models.fact_part import FactPart
from ...models.fact_sheet import FactSheet


async def create_fact_sheet(geometry: ogr.Geometry, orig_epsg: int, buffer: int) -> FactSheet:
    fact_list = await __create_fact_list(geometry, orig_epsg, buffer)

    return FactSheet(None, None, fact_list)


async def __create_fact_list(geometry: ogr.Geometry, orig_epsg: int, buffer: int) -> List[FactPart]:
    input_geom = create_buffered_geometry(geometry, buffer, DEFAULT_EPSG)
    tasks: List[asyncio.Task]

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(get_area_types(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer)),
            tg.create_task(get_buildings(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer)),
            tg.create_task(get_roads(
                input_geom, DEFAULT_EPSG, orig_epsg, buffer))
        ]

    result_list: List[FactPart] = []

    for task in tasks:
        result = task.result()

        if result is not None:
            result_list.append(result)

    return result_list
