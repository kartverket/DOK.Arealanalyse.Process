from typing import Literal
from .coverage_service import CoverageService


class CoverageArcGis(CoverageService):
    type: Literal['arcgis']
