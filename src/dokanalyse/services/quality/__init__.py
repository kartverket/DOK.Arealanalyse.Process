from typing import List
from ...utils.helpers.common import parse_string
from ...models.config.quality_indicator import QualityIndicator


def get_threshold_values(quality_indicator: QualityIndicator) -> List[str]:
    if not quality_indicator.warning_threshold:
        return []
    
    values = [value.strip()
              for value in quality_indicator.warning_threshold.split('OR')]
    result = list(map(lambda value: parse_string(value), values))

    return result
