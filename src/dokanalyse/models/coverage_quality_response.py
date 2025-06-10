

from typing import List, Dict
from .quality_measurement import QualityMeasurement


class CoverageQualityResponse():
    def __init__(self, quality_measurements: List[QualityMeasurement], warning_text: str, has_coverage: bool, is_relevant: bool, data: List[Dict]):
        self.quality_measurements = quality_measurements
        self.warning_text = warning_text
        self.has_coverage = has_coverage
        self.is_relevant = is_relevant
        self.data = data
