from typing import Union, Optional
from pydantic import FileUrl, HttpUrl
from .coverage_base_service import CoverageBaseService


class CoverageGeoPackage(CoverageBaseService):
    url: Union[FileUrl, HttpUrl]
    layer: str = '0'
    filter: Optional[str] = None