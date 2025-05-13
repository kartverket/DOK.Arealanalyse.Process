from typing import List, Optional
from pydantic import BaseModel, HttpUrl
import uuid


class CoverageService(BaseModel):
    url: HttpUrl
    layer: str
    geom_field: Optional[str] = None
    property: str
    planning_guidance_id: uuid.UUID = None
    building_guidance_id: uuid.UUID = None
    properties: List[str] = []    
