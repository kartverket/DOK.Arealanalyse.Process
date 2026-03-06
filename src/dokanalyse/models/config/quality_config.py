from uuid import UUID
from typing import List
from pydantic import BaseModel
from .quality_indicator import QualityIndicator


class QualityConfig(BaseModel):
    config_id: UUID | None = None
    indicators: List[QualityIndicator]
