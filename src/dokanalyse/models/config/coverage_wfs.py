from pydantic import BaseModel, HttpUrl


class CoverageWfs(BaseModel):
    url: HttpUrl
    layer: str
    geom_field: str
    property: str
