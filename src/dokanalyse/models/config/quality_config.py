from pydantic import BaseModel
import uuid
from typing import Optional, List
from .quality_indicator import QualityIndicator


class QualityConfig(BaseModel):
    config_id: Optional[uuid.UUID] = None
    indicators: List[QualityIndicator]
