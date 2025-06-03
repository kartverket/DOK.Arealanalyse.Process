from typing import List, Dict, Tuple
from osgeo import ogr
from . import get_threshold_values
from ..coverage import get_values_from_wfs, get_values_from_arcgis, get_values_from_geojson
from ..codelist import get_codelist
from ...models.quality_measurement import QualityMeasurement
from ...models.config.quality_indicator import QualityIndicator


async def get_coverage_quality(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[List[QualityMeasurement], str, bool, List[Dict]]:
    quality_data, has_coverage, data = await _get_coverage_quality_data(quality_indicator, geometry, epsg)

    if quality_data is None:
        return [], None, False, []

    measurements: List[QualityMeasurement] = []

    for value in quality_data.get('values'):
        measurements.append(QualityMeasurement(quality_data.get('id'), quality_data.get(
            'name'), value.get('value'), value.get('comment')))

    warning = quality_data.get('warning_text')

    return measurements, warning, has_coverage, data


async def _get_coverage_quality_data(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[Dict[str, any], bool, List[Dict]]:
    values, hit_area_percent, data = await _get_values_from_web_service(quality_indicator, geometry, epsg)

    if len(values) == 0 and quality_indicator.warning_threshold is not None:
        return None, False, []

    codelist = await get_codelist('fullstendighet_dekning')
    meas_values: List[Dict] = []

    if len(values) == 0 and quality_indicator.warning_threshold is None:
        comment = _get_label_from_codelist('ikkeKartlagt', codelist)

        meas_values.append({
            'value': 'Nei',
            'comment': comment
        })
    else:
        for value in values:
            if value == 'Ikke kartlagt':
                value = 'ikkeKartlagt'
            elif value == 'Ikke relevant':
                value = 'ikkeRelevant'

            meas_value = 'Nei' if value in [
                'ikkeKartlagt', 'ikkeRelevant'] else 'Ja'

            comment = _get_label_from_codelist(value, codelist)

            meas_values.append({
                'value': meas_value,
                'comment': comment
            })

    measurement = {
        'id': quality_indicator.quality_dimension_id,
        'name': quality_indicator.quality_dimension_name,
        'values': meas_values,
        'warning_text': _get_warning_text(quality_indicator, values, hit_area_percent)
    }

    has_coverage = _has_coverage(values, quality_indicator.warning_threshold)

    return measurement, has_coverage, data


async def _get_values_from_web_service(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float, List[Dict]]:
    if quality_indicator.wfs:
        return await get_values_from_wfs(quality_indicator.wfs, geometry, epsg)

    if quality_indicator.arcgis:
        return await get_values_from_arcgis(quality_indicator.arcgis, geometry, epsg)

    if quality_indicator.geojson:
        return await get_values_from_geojson(quality_indicator.geojson, geometry, epsg)
    
    if quality_indicator.gpkg:
        return await get_values_from_geojson(quality_indicator.gpkg, geometry, epsg)

    # TODO: Add support for OGC Features API

    return [], 0, []


def _get_warning_text(quality_indicator: QualityIndicator, values: List[str], hit_area_percent: float) -> str:
    threshold_values = get_threshold_values(quality_indicator)

    should_warn = any(value for value in values if any(
        t_value for t_value in threshold_values if t_value == value))

    should_warn = should_warn or len(
        values) == 0 and quality_indicator.warning_threshold is None

    # not_relevant = 'ikkeRelevant' in values or 'Ikke relevant' in values
    # should_warn = should_warn or not_relevant
    warning_text = None

    if should_warn:
        warning_text: str = quality_indicator.quality_warning_text

        if 0 < hit_area_percent < 100:
            hit_area = str(hit_area_percent).replace('.', ',')
            warning_text = f'{hit_area} % av {warning_text.lower()}'

    return warning_text


def _has_coverage(values: List[str], warning_threshold: str) -> bool:
    if len(values) == 0 and warning_threshold is None:
        return False

    has_value = any(value in ['ikkeKartlagt', 'Ikke kartlagt',
                    'ikkeRelevant', 'Ikke relevant'] for value in values)

    if has_value:
        has_other_values = any(
            value not in ['ikkeKartlagt', 'Ikke kartlagt', 'ikkeRelevant', 'Ikke relevant'] for value in values)
        return has_other_values

    return True


def _get_label_from_codelist(value: str, codelist: List[Dict]) -> str:
    if codelist is None or len(codelist) == 0:
        return None

    result = next(
        (entry for entry in codelist if entry['value'] == value), None)

    return result.get('label') if result is not None else None


__all__ = ['get_coverage_quality']
