from xmlschema import XMLSchema
from .coverage_service import CoverageService


class CoverageWfs(CoverageService):
    xml_schema: XMLSchema | None = None

    model_config = {
        'extra': 'ignore',
        'arbitrary_types_allowed': True
    }
