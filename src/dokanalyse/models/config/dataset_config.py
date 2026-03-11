from uuid import UUID
from typing import Dict, List, Literal
from pydantic import BaseModel, HttpUrl, model_validator
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
    wfs: HttpUrl | FeatureService | None = None
    arcgis: HttpUrl | FeatureService | None = None
    ogc_api: HttpUrl | FeatureService | None = None
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

    def get_feature_service_url(self) -> str:
        service = self.wfs or self.arcgis or self.ogc_api

        if isinstance(service, FeatureService):
            return str(service.url)

        return str(service)

    @model_validator(mode='before')
    @classmethod
    def check_service_type(cls, values: Dict) -> Dict:
        if not 'wfs' in values and not 'arcgis' in values and not 'ogc_api' in values:
            raise ValueError(
                'The dataset must have either the "wfs", "arcgis" or "ogc_api" property set')

        return values
