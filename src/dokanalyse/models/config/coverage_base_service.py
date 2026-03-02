from typing import List, Literal
from pydantic import BaseModel
import uuid


class CoverageBaseService(BaseModel):
    property: str
    planning_guidance_id: uuid.UUID | None = None
    building_guidance_id: uuid.UUID | None = None
    properties: List[str] | Literal['*'] = []
