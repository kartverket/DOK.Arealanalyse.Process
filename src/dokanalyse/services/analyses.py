import time
import traceback
from typing import List, Dict, Tuple, Any
from uuid import UUID, uuid4
import structlog
from structlog.stdlib import BoundLogger
import asyncio
import aiohttp
from socketio import SimpleClient
from osgeo import ogr
from pydash import kebab_case
from .dataset import get_config_ids, get_dataset_type
from .fact_sheet import create_fact_sheet
from .municipality import get_municipality
from .config import get_dataset_config, get_not_implemented_dataset_configs
from .map_image import generate_map_images
from .report import create_pdf
from .analysis_runner import run_analysis as run_single_analysis, run_empty_analysis, set_defaults as set_analysis_defaults
from ..services.file_storage import FileStorage, AzureBlobStorage, LocalFileShare
from ..utils.helpers.geometry import create_input_geometry, get_epsg_from_geojson
from ..models.state_emitter import StateEmitter, StateStatus
from ..models.config import DatasetConfig
from ..models import (Analysis, ArcGisAnalysis, OgcApiAnalysis,
                      WfsAnalysis, EmptyAnalysis, AnalysisResponse, ResultStatus)
from ..utils.correlation import get_correlation_id
from ..constants import (DEFAULT_EPSG, AZURE_BLOB_STORAGE_CONN_STR,
                               LOCAL_FILE_SHARE_DIR, LOCAL_FILE_SHARE_BASE_URL, DATASETS)

_logger: BoundLogger = structlog.get_logger(__name__)


async def run(data: Dict, sio_client: SimpleClient) -> Dict[str, Any]:       
    correlation_id = get_correlation_id()
    emitter = StateEmitter(correlation_id, sio_client)
    emitter.send_message(StateStatus.STARTING_UP)

    geojson = data['inputGeometry']
    geometry = create_input_geometry(geojson)
    orig_epsg = get_epsg_from_geojson(geojson)
    buffer: int = data.get('requestedBuffer', 0)
    context: str = data.get('context') or ''
    include_guidance: bool = data.get('includeGuidance', False)
    include_quality_measurement: bool = data.get(
        'includeQualityMeasurement', False)
    include_facts: bool = data.get('includeFacts', True)
    include_chosen_dok: bool = data.get('includeFilterChosenDOK', True)
    create_binaries: bool = data.get('createBinaries', True)

    municipality_number, municipality_name = await get_municipality(geometry, DEFAULT_EPSG)
    datasets = await _get_datasets(data, municipality_number)

    emitter.analyses_total = _get_datasets_to_analyze_count(datasets)
    emitter.send_message(StateStatus.ANALYZING_DATASETS)

    tasks: List[asyncio.Task] = []

    async with asyncio.TaskGroup() as tg:
        for config_id, should_analyze in datasets.items():
            task = tg.create_task(_run_analysis(
                config_id, should_analyze, geometry, DEFAULT_EPSG, orig_epsg, buffer,
                context, include_guidance, include_quality_measurement, emitter))
            tasks.append(task)

        if include_chosen_dok:
            not_implemented = get_not_implemented_dataset_configs()

            for config in not_implemented:
                task = tg.create_task(_run_not_implemented_analysis(config))
                tasks.append(task)

    fact_sheet = None

    if include_facts:
        emitter.send_message(StateStatus.CREATING_FACT_SHEET)
        fact_sheet = await create_fact_sheet(geometry, orig_epsg, buffer)

    response = AnalysisResponse.create(
        geojson, geometry, DEFAULT_EPSG, orig_epsg, buffer, fact_sheet, municipality_number, municipality_name)

    for task in tasks:
        result = task.result()

        if result:
            response.result_list.append(result)

    analyses_with_map_image = [
        analysis for analysis in response.result_list if analysis.raster_result_map]

    file_storage = _get_file_storage()
    dirname = str(uuid4())

    if create_binaries and file_storage:
        map_images = await generate_map_images(
            analyses_with_map_image, fact_sheet, emitter)
        await _upload_images(response, map_images, dirname, file_storage)

        emitter.send_message(StateStatus.CREATING_REPORT)
        report = create_pdf(response)
        response.report = await _upload_report(report, dirname, file_storage)

    return response.to_dict()


async def _run_analysis(
    config_id: UUID,
    should_analyze: bool,
    geometry: ogr.Geometry,
    epsg: int,
    orig_epsg: int,
    buffer: int,
    context: str,
    include_guidance: bool,
    include_quality_measurement: bool,
    emitter: StateEmitter
) -> Analysis | None:
    config = get_dataset_config(config_id)    

    if config is None:
        return None
    
    if not should_analyze:
        analysis = EmptyAnalysis(
            config.config_id, config, ResultStatus.NOT_RELEVANT)
        await run_empty_analysis(analysis)
        return analysis

    start = time.time()

    analysis = _create_analysis(
        config_id, config, geometry, epsg, orig_epsg, buffer)

    try:
        await run_single_analysis(analysis, context, include_guidance, include_quality_measurement)
    except Exception:
        await set_analysis_defaults(analysis)
        analysis.result_status = ResultStatus.ERROR
        end = time.time()

        err = traceback.format_exc()
        _logger.error('Analysis failed', config_id=str(
            config_id), dataset=config.name, duration=round(end - start, 2), error=err)
    else:
        end = time.time()
        _logger.info('Dataset analyzed', config_id=str(config_id),
                     dataset=config.name, duration=round(end - start, 2))

    emitter.send_message(StateStatus.DATASET_ANALYZED)

    return analysis


async def _run_not_implemented_analysis(config: DatasetConfig) -> Analysis:
    analysis = EmptyAnalysis(config.config_id, config,
                             ResultStatus.NOT_IMPLEMENTED)
    await run_empty_analysis(analysis)

    return analysis


def _create_analysis(
        config_id: UUID,
        config: DatasetConfig,
        geometry: ogr.Geometry,
        epsg: int,
        orig_epsg: int,
        buffer: int
) -> Analysis:
    dataset_type = get_dataset_type(config)

    match dataset_type:
        case 'arcgis':
            return ArcGisAnalysis(config_id, config, geometry, epsg, orig_epsg, buffer)
        case 'ogc_api':
            return OgcApiAnalysis(config_id, config, geometry, epsg, orig_epsg, buffer)
        case 'wfs':
            return WfsAnalysis(config_id, config, geometry, epsg, orig_epsg, buffer)
        case _:
            return None


async def _upload_images(
        response: AnalysisResponse,
        map_images: List[Tuple[str, str, bytes | None]],
        dirname: str,
        file_storage: FileStorage
) -> None:
    filtered = [map_image for map_image in map_images if map_image[2]]

    if not filtered:
        return

    await file_storage.create_dir(dirname)

    tasks: List[asyncio.Task[str]] = []

    async with asyncio.TaskGroup() as tg:
        for id, name, data in filtered:
            filename = f'{kebab_case(name)}.png'
            task = tg.create_task(file_storage.upload_binary(
                data, dirname, filename, content_type='image/png'), name=id)
            tasks.append(task)

    for task in tasks:
        task_name = task.get_name()

        if task_name == 'omraade':
            response.fact_sheet.raster_result_image = task.result()
            continue

        analysis = _find_analysis(response.result_list, task_name)

        if analysis:
            analysis.raster_result_image = task.result()


async def _upload_report(report: bytes, dirname: str, file_storage: FileStorage) -> str | None:
    await file_storage.create_dir(dirname)
    pdf_url = await file_storage.upload_binary(report, dirname, 'rapport.pdf', content_type='application/pdf')

    return pdf_url


async def _get_datasets(data: Dict, municipality_number: str) -> Dict[UUID, bool]:
    if not DATASETS:
        return await get_config_ids(data, municipality_number)

    datasets: Dict[UUID, bool] = {}

    for dataset in DATASETS.split(','):
        datasets[UUID(dataset)] = True

    return datasets


def _get_file_storage() -> FileStorage | None:
    if AZURE_BLOB_STORAGE_CONN_STR:
        return AzureBlobStorage(AZURE_BLOB_STORAGE_CONN_STR)

    if LOCAL_FILE_SHARE_DIR and LOCAL_FILE_SHARE_BASE_URL:
        return LocalFileShare(LOCAL_FILE_SHARE_DIR, LOCAL_FILE_SHARE_BASE_URL)

    return None


def _get_datasets_to_analyze_count(datasets: Dict[UUID, bool]) -> int:
    return len({key: value for (key, value) in datasets.items() if value == True})


def _find_analysis(analyses: List[Analysis], config_id: str) -> Analysis | None:
    return next((analysis for analysis in analyses if str(analysis.config_id) == config_id), None)


__all__ = ['run']
