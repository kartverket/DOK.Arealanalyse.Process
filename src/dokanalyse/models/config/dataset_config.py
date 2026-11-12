from uuid import UUID
from typing import List, Literal
from pydantic import BaseModel, HttpUrl
from xmlschema import XMLSchema
from .feature_service import FeatureService
from .layer import Layer


class DatasetConfig(BaseModel):
    config_id: UUID
    name: str
    comment: str | None = None
    disabled: bool | None = False
    title: str | None = None
    metadata_id: UUID | None = None
    features: FeatureService
    wms: HttpUrl
    layers: List[Layer] = []
    geom_field: str | None = None
    properties: List[str] | Literal['*'] = []
    skip_properties: List[str] = []
    themes: List[str]
    xml_schema: XMLSchema | None = None

    model_config = {
        'extra': 'ignore',
        'arbitrary_types_allowed': True
    }

    @property
    def service_type(self) -> Literal['wfs', 'arcgis', 'ogc_api']:
        return self.features.type

    def get_feature_service_url(self) -> str:
        return str(self.features.url)
