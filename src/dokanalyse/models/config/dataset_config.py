import uuid
from typing import Optional, List, Self
from pydantic import BaseModel, HttpUrl, model_validator
from .layer import Layer


class DatasetConfig(BaseModel):
    dataset_id: Optional[uuid.UUID] = None
    name: Optional[str] = None
    title: Optional[str] = None
    wfs: Optional[HttpUrl] = None
    arcgis: Optional[HttpUrl] = None
    ogc_api: Optional[HttpUrl] = None
    wms: HttpUrl
    layers: List[Layer]
    geom_field: Optional[str] = None
    properties: Optional[List[str]]
    themes: List[str]

    @model_validator(mode='after')
    def check_service_type(self) -> Self:
        if not self.wfs and not self.arcgis and not self.ogc_api:
            raise ValueError(
                'Datasettet må være av typen wfs, arcgis eller ogc_api')

        return self
