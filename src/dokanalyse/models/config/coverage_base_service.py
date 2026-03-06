from uuid import UUID
from typing import List, Literal
from pydantic import BaseModel


class CoverageBaseService(BaseModel):
    property: str
    planning_guidance_id: UUID | None = None
    building_guidance_id: UUID | None = None
    properties: List[str] | Literal['*'] = []
