from typing import List
from . import get_threshold_values
from ...models.quality_measurement import QualityMeasurement
from ...models.config.quality_indicator import QualityIndicator
from ...models.config.quality_indicator_type import QualityIndicatorType


def get_object_quality(quality_indicators: List[QualityIndicator], data: List[dict]) -> tuple[List[QualityMeasurement], List[str]]:
    quality_data = __get_object_quality_data(quality_indicators, data)
    measurements: List[QualityMeasurement] = []
    warnings: List[str] = []

    for entry in quality_data:
        value: dict

        for value in entry.get('values'):
            measurements.append(QualityMeasurement(entry.get('id'), entry.get(
                'name'), value.get('value'), value.get('comment')))

        warning = entry.get('warning_text')

        if warning is not None:
            warnings.append(warning)

    return measurements, warnings


def __get_object_quality_data(quality_indicators: List[QualityIndicator], data: List[dict]) -> List[dict]:
    if data is None or len(data) == 0:
        return []

    object_indicators = [
        indicator for indicator in quality_indicators if indicator.type == QualityIndicatorType.OBJECT]

    measurements: List[dict] = []

    for oi in object_indicators:
        prop = oi.property
        threshold_values = get_threshold_values(oi)
        values: List[dict] = []

        for entry in data:
            values.append({
                'value': entry[prop],
                'comment': None
            })

        distinct = list({value['value']: value for value in values}.values())

        should_warn = any(value['value'] for value in distinct if any(
            t_value for t_value in threshold_values if t_value == value['value']))

        measurements.append({
            'id': oi.quality_dimension_id,
            'name': oi.quality_dimension_name,
            'values': distinct,
            'warning_text': oi.quality_warning_text if should_warn else None
        })

    return measurements
