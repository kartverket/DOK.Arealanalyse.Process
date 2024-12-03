from pydantic import BaseModel
import uuid
from typing import Optional, List
from .quality_indicator import QualityIndicator


class QualityConfig(BaseModel):
    dataset_id: Optional[uuid.UUID] = None
    indicators: List[QualityIndicator]
