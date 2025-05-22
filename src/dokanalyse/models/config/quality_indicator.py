from pydantic import BaseModel, root_validator
from typing import Optional, Dict
from .quality_indicator_type import QualityIndicatorType
from . import CoverageService, CoverageGeoJson

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
    geojson: Optional[CoverageGeoJson] = None
    disabled: Optional[bool] = False

    @root_validator(pre=False)
    def check_coverage(cls, values: Dict) -> Dict:
        type = values.get('type')

        if type == QualityIndicatorType.COVERAGE and not 'wfs' in values and not 'arcgis' in values and not 'geojson' in values:
            raise ValueError(
                'If the quality indicator type is "coverage", either the properties "wfs", "arcgis" or "geojson" must be set')

        return values
