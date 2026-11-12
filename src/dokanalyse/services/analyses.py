import time
from typing import List, Dict, Tuple, Any
from uuid import UUID, uuid4
import structlog
from structlog.stdlib import BoundLogger
import asyncio
from socketio import SimpleClient
from osgeo import ogr
from pydash import kebab_case
from .dataset import get_config_ids
from .fact_sheet import create_fact_sheet
from .municipality import get_municipality
from .config import get_dataset_config, get_not_implemented_dataset_configs
from .map_image import generate_map_images
from .report import create_pdf
from .analysis_runner import run_analysis as run_single_analysis, set_defaults as set_analysis_defaults
from ..services.file_storage import FileStorage, AzureBlobStorage, LocalFileShare
from ..utils.helpers.geometry import create_input_geometry, get_epsg_from_geojson
from ..models.progress_emitter import ProgressEmitter, ProgressStatus
from ..models.config import DatasetConfig
from ..models import Analysis, AnalysisInput, EmptyAnalysis, AnalysisResponse, ResultStatus, FactSheet
from ..utils.correlation import get_correlation_id
from ..utils.async_executor import run_in_render_thread
from ..constants import (DEFAULT_EPSG, AZURE_BLOB_STORAGE_CONN_STR, AZURE_BLOB_STORAGE_CONTAINER,
                         LOCAL_FILE_SHARE_DIR, LOCAL_FILE_SHARE_BASE_URL, DATASETS)

_logger: BoundLogger = structlog.get_logger(__name__)


async def run(analysis_input: AnalysisInput, sio_client: SimpleClient | None) -> Dict[str, Any]:
    geojson = analysis_input.input_geometry
    buffer = analysis_input.requested_buffer
    context = analysis_input.context or ''
    include_guidance = analysis_input.include_guidance
    include_quality_measurement = analysis_input.include_quality_measurement
    include_facts = analysis_input.include_facts
    include_chosen_dok = analysis_input.include_filter_chosen_dok
    create_binaries = analysis_input.create_binaries

    correlation_id = get_correlation_id()
    file_storage = _get_file_storage()

    emitter = ProgressEmitter(
        correlation_id,
        sio_client,
        include_facts,
        create_binaries and file_storage is not None
    )

    emitter.send_message(ProgressStatus.STARTING_UP)
    geometry = create_input_geometry(geojson)
    orig_epsg = get_epsg_from_geojson(geojson)

    municipality = await get_municipality(geometry, DEFAULT_EPSG)

    if municipality is None:
        raise ValueError('Could not determine the municipality of the input geometry')

    municipality_number, municipality_name = municipality
    datasets = await _get_datasets(municipality_number, analysis_input.theme, include_chosen_dok)
    emitter.analyses_total = _get_datasets_to_analyze_count(datasets)
    emitter.send_message(ProgressStatus.STARTING_UP, True)

    tasks: List[asyncio.Task[Analysis | None]] = []
    fact_sheet_task: asyncio.Task[FactSheet | None] | None = None

    async with asyncio.TaskGroup() as tg:
        # The fact sheet is independent of the analyses and is created
        # concurrently. Progress is still reported after the analyses.
        if include_facts:
            fact_sheet_task = tg.create_task(_create_fact_sheet(geometry, orig_epsg, buffer))

        for config_id, should_analyze in datasets.items():
            task = tg.create_task(_run_analysis(
                config_id, should_analyze, geometry, DEFAULT_EPSG, orig_epsg, buffer,
                context, include_guidance, include_quality_measurement, emitter))
            tasks.append(task)

        if include_chosen_dok:
            not_implemented = await get_not_implemented_dataset_configs()

            for config in not_implemented:
                task = tg.create_task(_run_not_implemented_analysis(config))
                tasks.append(task)

    emitter.send_message(ProgressStatus.ANALYZING_DATASETS, True)
    fact_sheet = None

    if fact_sheet_task:
        emitter.send_message(ProgressStatus.CREATING_FACT_SHEET)
        fact_sheet = fact_sheet_task.result()
        emitter.send_message(ProgressStatus.CREATING_FACT_SHEET, True)

    response = AnalysisResponse.create(
        geojson, geometry, DEFAULT_EPSG, orig_epsg, buffer, fact_sheet, municipality_number, municipality_name)

    for task in tasks:
        result = task.result()

        if result:
            response.result_list.append(result)

    analyses_with_map_image = [
        analysis for analysis in response.result_list if analysis.raster_result_map]

    dirname = str(uuid4())

    if create_binaries and file_storage:
        try:
            map_images = await generate_map_images(
                analyses_with_map_image, fact_sheet, emitter)
            await _upload_images(response, map_images, dirname, file_storage)
            emitter.send_message(ProgressStatus.CREATING_MAP_IMAGES, True)

            emitter.send_message(ProgressStatus.CREATING_REPORT)
            report = await _create_report(response)

            if report:
                response.report = await _upload_report(report, dirname, file_storage)

            emitter.send_message(ProgressStatus.CREATING_REPORT, True)
        finally:
            await file_storage.close()

    return response.to_dict()


async def _run_analysis(
    config_id: UUID,
    should_analyze: bool,
    geometry: ogr.Geometry,
    epsg: int,
    orig_epsg: int | None,
    buffer: int,
    context: str,
    include_guidance: bool,
    include_quality_measurement: bool,
    emitter: ProgressEmitter
) -> Analysis | None:
    config = await get_dataset_config(config_id)

    if config is None:
        return None

    if not should_analyze:
        analysis = EmptyAnalysis(
            config.config_id, config, ResultStatus.NOT_RELEVANT)
        await set_analysis_defaults(analysis)
        return analysis

    start = time.time()

    analysis = Analysis(config_id, config, geometry, epsg, orig_epsg, buffer)

    try:
        await run_single_analysis(analysis, context, include_guidance, include_quality_measurement)
    except Exception as err:
        await set_analysis_defaults(analysis)
        duration = round(time.time() - start, 2)
        analysis.result_status = ResultStatus.ERROR
        analysis.status_code = 500
        analysis.duration = duration

        _logger.exception('Analysis failed', config_id=str(
            config_id), dataset=config.name, duration=duration, error=str(err))
    else:
        duration = round(time.time() - start, 2)
        analysis.duration = duration
        _logger.info('Dataset analyzed', config_id=str(config_id),
                     dataset=config.name, duration=duration)

    emitter.send_message(ProgressStatus.ANALYZING_DATASETS)

    return analysis


async def _create_fact_sheet(geometry: ogr.Geometry, orig_epsg: int | None, buffer: int) -> FactSheet | None:
    try:
        return await create_fact_sheet(geometry, orig_epsg, buffer)
    except Exception as err:
        _logger.exception('Creating fact sheet failed', error=str(err))
        return None


async def _create_report(response: AnalysisResponse) -> bytes | None:
    try:
        return await run_in_render_thread(create_pdf, response)
    except Exception as err:
        _logger.exception('Creating PDF report failed', error=str(err))
        return None


async def _run_not_implemented_analysis(config: DatasetConfig) -> Analysis | None:
    analysis = EmptyAnalysis(config.config_id, config,
                             ResultStatus.NOT_IMPLEMENTED)
    await set_analysis_defaults(analysis)

    if not analysis.run_on_dataset:
        return None

    return analysis


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

    tasks: List[asyncio.Task[str | None]] = []

    async with asyncio.TaskGroup() as tg:
        for id, name, data in filtered:
            if data is None:
                continue

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


async def _get_datasets(
        municipality_number: str,
        theme: str | None,
        include_chosen_dok: bool
) -> Dict[UUID, bool]:
    if not DATASETS:
        return await get_config_ids(municipality_number, theme, include_chosen_dok)

    datasets: Dict[UUID, bool] = {}

    for dataset in DATASETS.split(','):
        datasets[UUID(dataset)] = True

    return datasets


def _get_file_storage() -> FileStorage | None:
    if AZURE_BLOB_STORAGE_CONN_STR:
        return AzureBlobStorage(AZURE_BLOB_STORAGE_CONN_STR, AZURE_BLOB_STORAGE_CONTAINER)

    if LOCAL_FILE_SHARE_DIR and LOCAL_FILE_SHARE_BASE_URL:
        return LocalFileShare(LOCAL_FILE_SHARE_DIR, LOCAL_FILE_SHARE_BASE_URL)

    return None


def _get_datasets_to_analyze_count(datasets: Dict[UUID, bool]) -> int:
    return sum(1 for should_analyze in datasets.values() if should_analyze)


def _find_analysis(analyses: List[Analysis], config_id: str) -> Analysis | None:
    return next((analysis for analysis in analyses if str(analysis.config_id) == config_id), None)


__all__ = ['run']
