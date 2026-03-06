from uuid import UUID
from typing import Any, Dict, List, Tuple
from . import get_threshold_values
from ..dok_status import get_dok_status_for_dataset
from ...models.quality_measurement import QualityMeasurement
from ...models.config import DatasetConfig
from ...models.config.quality_indicator import QualityIndicator
from ...models.config.quality_indicator_type import QualityIndicatorType


async def get_dataset_quality(
    config: DatasetConfig,
    quality_indicators: List[QualityIndicator],
    **kwargs
) -> Tuple[List[QualityMeasurement], List[str]]:
    quality_data = await _get_dataset_quality_data(config, quality_indicators, kwargs)
    measurements: List[QualityMeasurement] = []
    warnings: List[str] = []

    for entry in quality_data:
        value: Dict[str, Any]

        for value in entry['values']:
            measurements.append(QualityMeasurement(
                entry['id'],
                entry['name'],
                value['value'],
                value['comment']
            ))

        warning = entry.get('warning_text')

        if warning is not None:
            warnings.append(warning)

    return measurements, warnings


async def _get_dataset_quality_data(
    config: DatasetConfig,
    quality_indicators: List[QualityIndicator],
    data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if not config.metadata_id:
        return []

    quality_measurements = await _get_dataset_quality_measurements(config.metadata_id)

    dataset_indicators = [
        indicator for indicator in quality_indicators if indicator.type == QualityIndicatorType.DATASET]

    measurements: List[Dict] = []

    for qm in quality_measurements:
        id = qm['quality_dimension_id']
        name = qm['quality_dimension_name']
        value = qm['value']

        measurement = {
            'id': id,
            'name': name,
            'values': [{
                'value': value,
                'comment': qm['comment']
            }],
            'warning_text': None
        }

        di = next(
            (di for di in dataset_indicators if di.quality_dimension_id == id), None)

        if di is not None:
            measurement['warning_text'] = _get_dataset_quality_warning_text(
                value, di, data)

        measurements.append(measurement)

    return measurements


async def _get_dataset_quality_measurements(metadata_id: UUID) -> List[Dict]:
    qms: List[Dict] = []

    dok_status = await get_dok_status_for_dataset(metadata_id)

    if dok_status is not None:
        qms.extend(dok_status['suitability'])

    return qms


def _get_dataset_quality_warning_text(value: Any, quality_indicator: QualityIndicator, data: Dict[str, Any]) -> str | None:
    predicate = quality_indicator.input_filter_func

    if predicate:
        result = predicate(data)

        if not result:
            return None

    threshold_values = get_threshold_values(quality_indicator)
    should_warn = value in threshold_values

    return quality_indicator.quality_warning_text if should_warn else None


__all__ = ['get_dataset_quality']
