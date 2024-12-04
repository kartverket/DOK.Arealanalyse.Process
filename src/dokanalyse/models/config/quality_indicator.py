from pydantic import BaseModel, root_validator
from typing import Optional, Dict
from .quality_indicator_type import QualityIndicatorType
from .coverage_wfs import CoverageWfs


class QualityIndicator(BaseModel):
    type: QualityIndicatorType
    quality_dimension_id: str
    quality_dimension_name: str
    quality_warning_text: str
    warning_threshold: str
    property: Optional[str] = None
    input_filter: Optional[str] = None
    wfs: Optional[CoverageWfs] = None

    @root_validator(pre=False)
    def check_coverage(cls, values: Dict) -> Dict:
        type, wfs = values.get('type'), values.get('wfs')

        if type == QualityIndicatorType.COVERAGE and wfs is None:
            raise ValueError(
                'If the quality indicator type is "coverage", the property "wfs" must be set')

        return values
