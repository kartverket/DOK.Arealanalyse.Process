import time
import logging
import traceback
from typing import List
from uuid import UUID
import asyncio
from socketio import SimpleClient
from osgeo import ogr
from .dataset import get_dataset_ids, get_dataset_type
from .fact_sheet import create_fact_sheet
from .municipality import get_municipality
from ..services.config import get_dataset_config
from ..utils.helpers.geometry import create_input_geometry, get_epsg
from ..models.config.dataset_config import DatasetConfig
from ..models.analysis import Analysis
from ..models.arcgis_analysis import ArcGisAnalysis
from ..models.empty_analysis import EmptyAnalysis
from ..models.ogc_api_analysis import OgcApiAnalysis
from ..models.wfs_analysis import WfsAnalysis
from ..models.analysis_response import AnalysisResponse
from ..models.result_status import ResultStatus
from ..utils.constants import DEFAULT_EPSG
from ..utils.correlation_id_middleware import get_correlation_id

_LOGGER = logging.getLogger(__name__)


async def run(data: dict, sio_client: SimpleClient) -> AnalysisResponse:
    geo_json = data.get('inputGeometry')
    geometry = create_input_geometry(geo_json)
    orig_epsg = get_epsg(geo_json)
    buffer = data.get('requestedBuffer', 0)
    context = data.get('context')
    include_guidance = data.get('includeGuidance', False)
    include_quality_measurement = data.get('includeQualityMeasurement', False)
    municipality_number, municipality_name = await get_municipality(geometry, DEFAULT_EPSG)

    datasets = await get_dataset_ids(data, municipality_number)
    correlation_id = get_correlation_id()

    if correlation_id and sio_client:
        to_analyze = {key: value for (
            key, value) in datasets.items() if value == True}
        sio_client.emit('datasets_counted_api', {'count': len(
            to_analyze), 'recipient': correlation_id})

    tasks: List[asyncio.Task] = []

    async with asyncio.TaskGroup() as tg:
        for dataset_id, should_analyze in datasets.items():
            task = tg.create_task(_run_analysis(
                dataset_id, should_analyze, geometry, DEFAULT_EPSG, orig_epsg, buffer,
                context, include_guidance, include_quality_measurement, sio_client))
            tasks.append(task)

    fact_sheet = await create_fact_sheet(geometry, orig_epsg, buffer)

    response = AnalysisResponse.create(
        geo_json, geometry, DEFAULT_EPSG, orig_epsg, buffer, fact_sheet, municipality_number, municipality_name)

    for task in tasks:
        response.result_list.append(task.result())

    return response.to_dict()


async def _run_analysis(dataset_id: UUID, should_analyze: bool, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int,
                       context: str, include_guidance: bool, include_quality_measurement: bool, sio_client: SimpleClient) -> Analysis:
    config = get_dataset_config(dataset_id)

    if config is None:
        return None

    if not should_analyze:
        analysis = EmptyAnalysis(config.dataset_id, config, ResultStatus.NOT_RELEVANT)
        await analysis.run()
        return analysis

    start = time.time()
    correlation_id = get_correlation_id()

    analysis = _create_analysis(
        dataset_id, config, geometry, epsg, orig_epsg, buffer)

    try:
        await analysis.run(context, include_guidance, include_quality_measurement)
    except Exception:
        err = traceback.format_exc()
        _LOGGER.error(err)
        await analysis.set_default_data()
        analysis.result_status = ResultStatus.ERROR

    end = time.time()
    print(f'{dataset_id} - {config.name}: {round(end - start, 2)} sek.')

    if correlation_id and sio_client:
        sio_client.emit('dataset_analyzed_api', {
            'dataset': dataset_id, 'recipient': correlation_id})

    return analysis


def _create_analysis(dataset_id: UUID, config: DatasetConfig, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int) -> Analysis:
    dataset_type = get_dataset_type(config)
    
    match dataset_type:
        case 'arcgis':
            return ArcGisAnalysis(dataset_id, config, geometry, epsg, orig_epsg, buffer)
        case 'ogc_api':
            return OgcApiAnalysis(dataset_id, config, geometry, epsg, orig_epsg, buffer)
        case 'wfs':
            return WfsAnalysis(dataset_id, config, geometry, epsg, orig_epsg, buffer)
        case _:
            return None
