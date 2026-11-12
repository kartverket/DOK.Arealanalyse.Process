from typing import Literal
from pydantic import FileUrl, HttpUrl
from .coverage_base_service import CoverageBaseService


class CoverageGeoJson(CoverageBaseService):
    type: Literal['geojson']
    url: FileUrl | HttpUrl
    layer: str = '0'
    filter: str | None = None
