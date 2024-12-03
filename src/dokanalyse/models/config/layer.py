from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional, Self
from ..result_status import ResultStatus
import uuid


class Layer(BaseModel):
    wfs: Optional[str] = None
    arcgis: Optional[str] = None
    ogc_api: Optional[str] = None
    wms: List[str]
    filter: Optional[str] = None
    result_status: ResultStatus
    geolett_id: Optional[uuid.UUID] = None

    @field_validator('result_status')
    @classmethod
    def check_result_status(cls, value: ResultStatus) -> ResultStatus:
        valid_statuses = [
            ResultStatus.NO_HIT_GREEN,
            ResultStatus.NO_HIT_YELLOW,
            ResultStatus.HIT_YELLOW,
            ResultStatus.HIT_RED
        ]

        if value not in valid_statuses:
            raise ValueError(
                f'The layer\'s result_status must be either {', '.join(list(map(lambda status: status.value, valid_statuses)))}')

        return value

    @model_validator(mode='after')
    def check_layer_type(self) -> Self:
        if not self.wfs and not self.arcgis and not self.ogc_api:
            raise ValueError(
                'The layer must have either the "wfs", "arcgis" or "ogc_api" property set')
            
        return self
