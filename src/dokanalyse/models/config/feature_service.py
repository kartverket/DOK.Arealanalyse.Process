from typing import Optional
from pydantic import BaseModel, HttpUrl
from .auth import ApiKey, Basic


class FeatureService(BaseModel):
    url: HttpUrl
    auth: Optional[ApiKey | Basic] = None

