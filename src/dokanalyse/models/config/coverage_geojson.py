from typing import List
from pydantic import BaseModel, AnyUrl
import uuid


class CoverageGeoJson(BaseModel):
    url: AnyUrl
    property: str
    planning_guidance_id: uuid.UUID = None
    building_guidance_id: uuid.UUID = None
    properties: List[str] = []    
