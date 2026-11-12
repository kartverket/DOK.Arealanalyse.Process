from typing import Literal
from pydantic import BaseModel, HttpUrl
from .auth import ApiKey, Basic


class FeatureService(BaseModel):
    type: Literal['wfs', 'arcgis', 'ogc_api']
    url: HttpUrl
    auth: ApiKey | Basic | None = None
