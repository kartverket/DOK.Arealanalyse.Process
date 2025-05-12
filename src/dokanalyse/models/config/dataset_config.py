import uuid
from typing import Optional, List, Dict
from pydantic import BaseModel, HttpUrl, root_validator
from .layer import Layer


class DatasetConfig(BaseModel):
    config_id: Optional[uuid.UUID] = None
    metadata_id: Optional[uuid.UUID] = None
    name: Optional[str] = None
    title: Optional[str] = None
    disabled: Optional[bool] = False
    wfs: Optional[HttpUrl] = None
    arcgis: Optional[HttpUrl] = None
    ogc_api: Optional[HttpUrl] = None
    wms: HttpUrl
    layers: List[Layer]
    geom_field: Optional[str] = None
    properties: Optional[List[str]]
    themes: List[str]

    @root_validator(pre=True)
    def check_service_type(cls, values: Dict) -> Dict:
        if not 'wfs' in values and not 'arcgis' in values and not 'ogc_api' in values:
            raise ValueError(
                'The dataset must have either the "wfs", "arcgis" or "ogc_api" property set')

        return values
