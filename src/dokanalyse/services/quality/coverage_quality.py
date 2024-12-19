from typing import List, Dict, Tuple
from osgeo import ogr
from . import get_threshold_values
from ..coverage import get_values_from_wfs
from ..codelist import get_codelist
from ...models.quality_measurement import QualityMeasurement
from ...models.config.quality_indicator import QualityIndicator


async def get_coverage_quality(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[List[QualityMeasurement], str, bool]:
    quality_data, has_coverage = await _get_coverage_quality_data(quality_indicator, geometry, epsg)

    if quality_data is None:
        return [], None, False

    measurements: List[QualityMeasurement] = []

    for value in quality_data.get('values'):
        measurements.append(QualityMeasurement(quality_data.get('id'), quality_data.get(
            'name'), value.get('value'), value.get('comment')))

    warning = quality_data.get('warning_text')

    return measurements, warning, has_coverage


async def _get_coverage_quality_data(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[Dict[str, any], bool]:
    values, hit_area_percent = await _get_values_from_web_service(quality_indicator, geometry, epsg)

    if len(values) == 0:
        return None, False

    codelist = await get_codelist('fullstendighet_dekning')
    meas_values: List[Dict] = []

    for value in values:
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

    return measurement, _has_coverage(values)


async def _get_values_from_web_service(quality_indicator: QualityIndicator, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float]:
    if quality_indicator.wfs is not None:
        return await get_values_from_wfs(quality_indicator.wfs, geometry, epsg)

    # TODO: Add support for ArcGIS and OGC Features API

    return [], 0


def _get_warning_text(quality_indicator: QualityIndicator, values: List[str], hit_area_percent: float) -> str:
    threshold_values = get_threshold_values(quality_indicator)

    should_warn = any(value for value in values if any(
        t_value for t_value in threshold_values if t_value == value))

    warning_text = None

    if should_warn:
        warning_text: str = quality_indicator.quality_warning_text

        if 0 < hit_area_percent < 100:
            hit_area = str(hit_area_percent).replace('.', ',')
            warning_text = f'{hit_area} % av {warning_text.lower()}'

    return warning_text


def _has_coverage(values: List[str]) -> bool:
    if 'ikkeKartlagt' in values:
        has_other_values = any(
            value != 'ikkeKartlagt' for value in values)
        return has_other_values

    return True


def _get_label_from_codelist(value: str, codelist: List[Dict]) -> str:
    if codelist is None or len(codelist) == 0:
        return None

    result = next(
        (entry for entry in codelist if entry['value'] == value), None)

    return result.get('label') if result is not None else None


__all__ = ['get_coverage_quality']