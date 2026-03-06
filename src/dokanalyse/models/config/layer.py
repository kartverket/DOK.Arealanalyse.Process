from uuid import UUID
from pydantic import BaseModel, model_validator, field_validator
from typing import List, Dict, Callable
from ..result_status import ResultStatus


class Layer(BaseModel):
    wfs: str | None = None
    arcgis: str | None = None
    ogc_api: str | None = None
    wms: List[str]
    filter: str | None = None
    filter_func: Callable[[Dict], bool] | None = None
    result_status: ResultStatus
    planning_guidance_id: UUID | None = None
    building_guidance_id: UUID | None = None

    model_config = {
        'extra': 'ignore'
    }

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
            raise ValueError('The layer\'s result_status must be either ' + 
                             ', '.join(list(map(lambda status: status.value, valid_statuses))))

        return value

    @model_validator(mode='before')
    @classmethod
    def check_layer_type(cls, values: Dict) -> Dict:
        if not 'wfs' in values and not 'arcgis' in values and not 'ogc_api' in values:
            raise ValueError(
               'The layer must have either the "wfs", "arcgis" or "ogc_api" property set')

        return values
