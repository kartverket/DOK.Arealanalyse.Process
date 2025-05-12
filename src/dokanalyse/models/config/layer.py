from pydantic import BaseModel, root_validator, validator
from typing import List, Optional, Dict
from ..result_status import ResultStatus
import uuid


class Layer(BaseModel):
    wfs: Optional[str] = None
    arcgis: Optional[str] = None
    ogc_api: Optional[str] = None
    wms: List[str]
    filter: Optional[str] = None
    result_status: ResultStatus
    planning_guidance_id: Optional[uuid.UUID] = None
    building_guidance_id: Optional[uuid.UUID] = None

    @validator('result_status')
    def check_result_status(cls, value: ResultStatus) -> ResultStatus:
        valid_statuses = [
            ResultStatus.NO_HIT_GREEN,
            ResultStatus.NO_HIT_YELLOW,
            ResultStatus.HIT_YELLOW,
            ResultStatus.HIT_RED
        ]

        if value not in valid_statuses:
            raise ValueError('The layer\'s result_status must be either ' + 
                             ', '.join(list(map(lambda status: status.value, valid_statuses))))

        return value

    @root_validator(pre=True)
    def check_layer_type(cls, values: Dict) -> Dict:
        if not 'wfs' in values and not 'arcgis' in values and not 'ogc_api' in values:
            raise ValueError(
               'The layer must have either the "wfs", "arcgis" or "ogc_api" property set')

        return values
