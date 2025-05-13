from pydantic import BaseModel, root_validator
from typing import Optional, Dict
from .quality_indicator_type import QualityIndicatorType
from .coverage_service import CoverageService


class QualityIndicator(BaseModel):
    type: QualityIndicatorType
    quality_dimension_id: str
    quality_dimension_name: str
    quality_warning_text: str
    warning_threshold: Optional[str]
    property: Optional[str] = None
    input_filter: Optional[str] = None
    wfs: Optional[CoverageService] = None
    arcgis: Optional[CoverageService] = None
    disabled: Optional[bool] = False

    @root_validator(pre=False)
    def check_coverage(cls, values: Dict) -> Dict:
        type = values.get('type')

        if type == QualityIndicatorType.COVERAGE and not 'wfs' in values and not 'arcgis' in values:
            raise ValueError(
                'If the quality indicator type is "coverage", either the property "wfs" or "arcgis" must be set')

        return values
