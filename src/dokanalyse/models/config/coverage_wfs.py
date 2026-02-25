from typing import Optional
from xmlschema import XMLSchema
from .coverage_service import CoverageService


class CoverageWfs(CoverageService):
    xml_schema: Optional[XMLSchema] = None

    model_config = {
        'extra': 'ignore',
        'arbitrary_types_allowed': True
    }
