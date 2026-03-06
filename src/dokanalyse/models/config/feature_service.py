from pydantic import BaseModel, HttpUrl
from .auth import ApiKey, Basic


class FeatureService(BaseModel):
    url: HttpUrl
    auth: ApiKey | Basic | None = None

