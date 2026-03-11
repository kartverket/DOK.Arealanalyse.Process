from typing import List, Literal
from pydantic import BaseModel, HttpUrl
from enum import Enum
from .auth import ApiKey, Basic


class FeatureServiceType(str, Enum):
    WFS = 'wfs'
    ArcGIS = 'arcgis'
    OGC_API = 'ogcapi'


class FeatureBaseService(BaseModel):
    type: FeatureServiceType
    url: HttpUrl
    auth: ApiKey | Basic | None = None


class WfsService(FeatureBaseService):
    type: FeatureServiceType = FeatureServiceType.WFS


class ArcGisService(FeatureBaseService):
    type: FeatureServiceType = FeatureServiceType.ArcGIS


class OgcApiService(FeatureBaseService):
    type: FeatureServiceType = FeatureServiceType.OGC_API
