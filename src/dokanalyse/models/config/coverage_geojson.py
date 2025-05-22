from typing import Union
from pydantic import BaseModel, FileUrl, HttpUrl
from .coverage_base_service import CoverageBaseService


class CoverageGeoJson(CoverageBaseService):
    url: Union[FileUrl, HttpUrl]
    layer: str = '0'
