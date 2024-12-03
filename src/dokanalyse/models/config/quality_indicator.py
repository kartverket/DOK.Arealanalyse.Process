from pydantic import BaseModel, model_validator
from typing import Optional, Self
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

    @model_validator(mode='after')
    def check_coverage(self) -> Self:
        if self.type == QualityIndicatorType.COVERAGE and self.wfs == None:
            raise ValueError(
                'If the quality indicator type is "coverage", the property "wfs" must be set')

        return self
