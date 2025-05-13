from typing import List
from pydantic import BaseModel, HttpUrl
import uuid

class CoverageArcGis(BaseModel):
    url: HttpUrl
    layer: str
    property: str
    planning_guidance_id: uuid.UUID = None
    building_guidance_id: uuid.UUID = None
    properties: List[str] = []
