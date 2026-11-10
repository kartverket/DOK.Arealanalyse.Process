from uuid import UUID
from typing import List, Self
from pydantic import BaseModel, model_validator
from .quality_indicator import QualityIndicator
from .quality_indicator_type import QualityIndicatorType


class QualityConfig(BaseModel):
    config_id: UUID | None = None
    indicators: List[QualityIndicator]

    @model_validator(mode='after')
    def check_single_coverage_indicator(self) -> Self:
        coverage_indicators = [
            indicator for indicator in self.indicators
            if indicator.type == QualityIndicatorType.COVERAGE and not indicator.disabled]

        if len(coverage_indicators) > 1:
            raise ValueError('A dataset can only have one coverage quality indicator')

        return self
