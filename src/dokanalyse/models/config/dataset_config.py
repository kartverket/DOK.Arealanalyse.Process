import uuid
from typing import Optional, List
from pydantic import BaseModel, HttpUrl
from xmlschema import XMLSchema
from .feature_service import FeatureService
from .layer import Layer


class DatasetConfig(BaseModel):
    config_id: Optional[uuid.UUID] = None
    metadata_id: Optional[uuid.UUID] = None
    name: Optional[str] = None
    title: Optional[str] = None
    disabled: Optional[bool] = False
    wfs: Optional[HttpUrl | FeatureService] = None
    arcgis: Optional[HttpUrl | FeatureService] = None
    ogc_api: Optional[HttpUrl | FeatureService] = None
    wms: Optional[HttpUrl] = None
    layers: Optional[List[Layer]] = []
    geom_field: Optional[str] = None
    properties: Optional[List[str]] = []
    skip_properties: Optional[List[str]] = []
    themes: List[str]
    xml_schema: Optional[XMLSchema] = None

    model_config = {
        'extra': 'ignore',
        'arbitrary_types_allowed': True
    }

    def get_feature_service_url(self) -> str:
        service = self.wfs or self.arcgis or self.ogc_api

        if isinstance(service, FeatureService):
            return str(service.url)

        return str(service)

