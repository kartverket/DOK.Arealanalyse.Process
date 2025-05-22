from typing import List
from pydantic import BaseModel
import uuid


class CoverageBaseService(BaseModel):
    property: str
    planning_guidance_id: uuid.UUID = None
    building_guidance_id: uuid.UUID = None
    properties: List[str] = []    
