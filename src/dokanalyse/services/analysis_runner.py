import sys
from uuid import UUID
from .config import get_quality_indicator_configs
from .kartkatalog import get_kartkatalog_metadata
from .quality.coverage_quality import get_coverage_quality
from .quality.dataset_quality import get_dataset_quality
from .quality.object_quality import get_object_quality
from .guidance_data import get_guidance_data
from .raster_result import get_wms_url, get_cartography_url
from .query_strategies import get_query_strategy
from ..models.analysis import Analysis
from ..models.config import Layer
from ..models.result_status import ResultStatus
from ..utils.helpers.geometry import create_buffered_geometry, create_run_on_input_geometry_json
from ..utils.helpers.quality import get_coverage_indicator, get_coverage_service_config_data


async def run_analysis(
    analysis: Analysis,
    context: str,
    include_guidance: bool,
    include_quality_measurement: bool
) -> None:
    analysis.set_input_geometry()

    await _run_coverage_analysis(analysis, context)

    if analysis.has_coverage:
        await _run_queries(analysis, context)

        if analysis.result_status == ResultStatus.TIMEOUT or analysis.result_status == ResultStatus.ERROR:
            await set_defaults(analysis)
            return
    elif not analysis.is_relevant:
        analysis.result_status = ResultStatus.NO_HIT_GREEN
    else:
        analysis.result_status = ResultStatus.NO_HIT_YELLOW

    if analysis.result_status in [ResultStatus.NO_HIT_GREEN, ResultStatus.NO_HIT_YELLOW] and analysis.is_relevant:
        await _set_distance_to_object(analysis)

    analysis.calculate_geometry_areas()

    analysis.run_algorithm.append('deliver result')

    analysis.run_on_input_geometry_json = create_run_on_input_geometry_json(
        analysis.run_on_input_geometry, analysis.epsg, analysis.orig_epsg)

    await set_defaults(analysis)

    if include_guidance and analysis.guidance_data is not None:
        _apply_guidance(analysis)

    if include_quality_measurement:
        await _evaluate_quality(analysis, context)


async def set_defaults(analysis: Analysis) -> None:
    analysis.title = analysis.guidance_data.get(
        'title') if analysis.guidance_data else analysis.config.title
    analysis.themes = analysis.config.themes
    analysis.run_on_dataset = await get_kartkatalog_metadata(analysis.config.metadata_id)


async def _run_queries(analysis: Analysis, context: str) -> None:
    strategy = get_query_strategy(analysis)

    if strategy is None:
        return

    first_layer = analysis.config.layers[0]
    guidance_id = _get_guidance_id(first_layer, context)
    guidance_data = await get_guidance_data(guidance_id)

    analysis.run_algorithm.append(
        f'query {strategy.get_service_url(analysis.config)}')

    for layer in analysis.config.layers:
        if layer.filter is not None:
            analysis.run_algorithm.append(f'add filter {layer.filter}')

        layer_name = strategy.get_layer_name(layer)

        status_code, api_response = await strategy.query(
            analysis.config, layer, analysis.run_on_input_geometry, analysis.epsg)

        if status_code == 408:
            analysis.result_status = ResultStatus.TIMEOUT
            analysis.run_algorithm.append(
                f'intersects layer {layer_name} (Timeout)')
            break
        elif status_code != 200:
            analysis.result_status = ResultStatus.ERROR
            analysis.run_algorithm.append(
                f'intersects layer {layer_name} (Error)')
            break

        if api_response:
            parsed = strategy.parse_response(
                api_response, analysis.config, layer)

            if len(parsed['properties']) > 0:
                analysis.run_algorithm.append(
                    f'intersects layer {layer_name} (True)')

                guidance_id = _get_guidance_id(layer, context)
                guidance_data = await get_guidance_data(guidance_id)

                analysis.data = parsed['properties']
                analysis.geometries = parsed['geometries']
                analysis.raster_result_map = get_wms_url(
                    str(analysis.config.wms), layer.wms)
                analysis.cartography = await get_cartography_url(
                    str(analysis.config.wms), layer.wms, analysis.run_on_input_geometry)
                analysis.result_status = layer.result_status
                break

            analysis.run_algorithm.append(
                f'intersects layer {layer_name} (False)')

    analysis.guidance_data = guidance_data


async def _set_distance_to_object(analysis: Analysis) -> None:
    strategy = get_query_strategy(analysis)

    if strategy is None:
        return

    buffered_geom = create_buffered_geometry(
        analysis.geometry, 20000, analysis.epsg)
    layer = analysis.config.layers[0]

    _, response = await strategy.query(
        analysis.config, layer, buffered_geom, analysis.epsg)

    if response is None:
        analysis.distance_to_object = sys.maxsize
        return

    geometries = strategy.extract_geometries(response, analysis.config, layer)
    distances = []

    for geom in geometries:
        distance = round(analysis.run_on_input_geometry.Distance(geom))
        distances.append(distance)

    distances.sort()
    analysis.run_algorithm.append('get distance to nearest object')

    if len(distances) == 0:
        analysis.distance_to_object = sys.maxsize
    else:
        analysis.distance_to_object = distances[0]


async def _run_coverage_analysis(analysis: Analysis, context: str) -> None:
    quality_indicators = await get_quality_indicator_configs(analysis.config_id)
    ci = get_coverage_indicator(quality_indicators)

    if not ci:
        return

    coverage_svc = get_coverage_service_config_data(ci)

    if not coverage_svc:
        return

    analysis.run_algorithm.append(f'check coverage {coverage_svc.get("url")}')
    response = await get_coverage_quality(ci, analysis.run_on_input_geometry, analysis.epsg)
    analysis.run_algorithm.append(
        f'intersects layer {coverage_svc.get("layer")} ({response.has_coverage})')

    if not response.has_coverage:
        analysis.data = response.data
        guidance_id = coverage_svc.get(
            'building_guidance_id') if context.lower() == 'byggesak' else coverage_svc.get('planning_guidance_id')

        if guidance_id:
            analysis.guidance_data = await get_guidance_data(guidance_id)

    analysis.quality_measurement.extend(response.quality_measurements)
    analysis.has_coverage = response.has_coverage
    analysis.is_relevant = response.is_relevant

    if response.warning_text is not None:
        analysis.quality_warning.append(response.warning_text)


def _apply_guidance(analysis: Analysis) -> None:
    if not analysis.guidance_data:
        return

    if analysis.result_status != ResultStatus.NO_HIT_GREEN:
        analysis.description = analysis.guidance_data.get('description')
        analysis.guidance_text = analysis.guidance_data.get('guidance_text')

    analysis.guidance_uri = analysis.guidance_data.get('guidance_uri', [])
    analysis.possible_actions = analysis.guidance_data.get(
        'possible_actions', [])


async def _evaluate_quality(analysis: Analysis, context: str) -> None:
    quality_indicators = await get_quality_indicator_configs(analysis.config_id)

    if len(quality_indicators) == 0:
        return

    dataset_qms, dataset_warnings = await get_dataset_quality(analysis.config, quality_indicators, context=context, themes=analysis.themes)
    object_qms, object_warnings = [], []

    if analysis.has_coverage:
        object_qms, object_warnings = get_object_quality(
            quality_indicators, analysis.data)

    analysis.quality_measurement.extend(dataset_qms)
    analysis.quality_measurement.extend(object_qms)
    analysis.quality_warning.extend(dataset_warnings)
    analysis.quality_warning.extend(object_warnings)


def _get_guidance_id(layer: Layer, context: str) -> UUID | None:
    if context.lower() == 'byggesak':
        return layer.building_guidance_id

    return layer.planning_guidance_id


__all__ = ['run_analysis', 'set_defaults']
