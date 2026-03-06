from typing import Callable, Dict
from pydantic import BaseModel, model_validator
from .quality_indicator_type import QualityIndicatorType
from . import CoverageService, CoverageWfs, CoverageGeoJson, CoverageGeoPackage


class QualityIndicator(BaseModel):
    type: QualityIndicatorType
    quality_dimension_id: str
    quality_dimension_name: str
    quality_warning_text: str | None = None
    warning_threshold: str | None = None
    property: str | None = None
    input_filter: str | None = None
    wfs: CoverageWfs | None = None
    arcgis: CoverageService | None = None
    geojson: CoverageGeoJson | None = None
    gpkg: CoverageGeoPackage | None = None
    disabled: bool | None = False
    input_filter_func: Callable[[Dict], bool] | None = None

    model_config = {
        'extra': 'ignore'
    }

    @model_validator(mode='before')
    @classmethod
    def check_coverage(cls, values: Dict) -> Dict:
        type = values.get('type')

        if type == QualityIndicatorType.COVERAGE and not 'wfs' in values and not 'arcgis' in values and not 'geojson' in values and not 'gpkg' in values:
            raise ValueError(
                'If the quality indicator type is "coverage", either the properties "wfs", "arcgis", "geojson" or "gpkg" must be set')

        return values
