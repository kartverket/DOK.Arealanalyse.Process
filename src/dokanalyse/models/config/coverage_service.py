from typing import Optional
from pydantic import HttpUrl
from .coverage_base_service import CoverageBaseService


class CoverageService(CoverageBaseService):
    url: HttpUrl
    layer: str
    geom_field: Optional[str] = None
