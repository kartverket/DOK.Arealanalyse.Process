from typing import Dict, Tuple
import asyncio
from osgeo import ogr, osr
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .services import analyses
from .utils.helpers.request import request_is_valid
from .utils.socket_io import get_client
from .utils import logger

logger.setup()

osr.UseExceptions()
ogr.UseExceptions()

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'dokanalyse',
    'title': {
        'no': 'DOK-analyse'
    },
    'description': {
        'no': 'Tjeneste som utfører en en standardisert DOK-arealanalyse for enhetlig DOK-analyse på tvers av kommuner og systemleverandørerviser.',
    },
    'keywords': [
        'dokanalyse',
        'DOK'
    ],
    'links': [],
    'inputs': {
        'inputGeometry': {
            'title': 'Område',
            'description': 'Området en ønsker å analysere mot. Kan f.eks. være en eiendom eller et planområde.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'requestedBuffer': {
            'title': 'Ønsket buffer',
            'description': 'Antall meter som legges på inputGeometry som buffer i analysen. Kan utelates og avgjøres av analysen hva som er fornuftig buffer.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'context': {
            'title': 'Kontekst',
            'description': 'Hint om hva analysen skal brukes til.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'theme': {
            'title': 'Tema',
            'description': 'DOK-tema kan angis for å begrense analysen til aktuelle tema.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'includeGuidance': {
            'title': 'Inkluder veiledning',
            'description': 'Velg om veiledningstekster skal inkluderes i resultat om det finnes i Geolett-registeret. Kan være avhengig av å styres med context for å få riktige tekster.',
            'schema': {
                'type': 'boolean'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'includeQualityMeasurement': {
            'title': 'Inkluder kvalitetsinformasjon',
            'description': 'Velg om kvalitetsinformasjon skal tas med i resultatet der det er mulig, slik som dekningskart, egnethet, nøyaktighet, osv.',
            'schema': {
                'type': 'boolean'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'includeFilterChosenDOK': {
            'title': 'Inkluder kun kommunens valgte DOK-data',
            'schema': {
                'type': 'boolean'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'includeFactSheet': {
            'title': 'Inkluder faktainformasjon',
            'schema': {
                'type': 'boolean'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'resultList': {
            'title': 'Resultatliste',
            'description': 'Strukturert resultat på analysen',
            'schema': {
                'type': 'Result',
            },
            'minOccurs': 0
        },
        'report': {
            'title': 'Rapport',
            'description': 'Rapporten levert som PDF',
            'schema': {
                'type': 'binary',
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'inputGeometry': {
            'title': 'Område',
            'description': 'Valgt område for analyse',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        }
    },
    'example': {
        'inputs': {
            'inputGeometry': {
                'type': 'Polygon',
                'coordinates': [
                    [
                        [
                            504132.67,
                            6585575.94
                        ],
                        [
                            504137.07,
                            6585483.64
                        ],
                        [
                            504286.5,
                            6585488.04
                        ],
                        [
                            504273.32,
                            6585575.94
                        ],
                        [
                            504132.67,
                            6585575.94
                        ]
                    ]
                ],
                'crs': {
                    'type': 'name',
                    'properties': {
                        'name': 'urn:ogc:def:crs:EPSG::25832'
                    }
                }
            },
            'requestedBuffer': 50,
            'context': 'Reguleringsplan',
            'theme': 'Natur',
            'includeGuidance': True,
            'includeQualityMeasurement': True,
            'includeFilterChosenDOK': True,
            'includeFactSheet': True
        }
    }
}


class DokanalyseProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: Dict, outputs=None) -> Tuple[str, Dict]:
        mimetype = 'application/json'

        if not request_is_valid(data):
            raise ProcessorExecuteError('Invalid payload')

        sio_client = get_client()

        try:
            outputs = asyncio.run(analyses.run(data, sio_client))
        finally:
            if sio_client:
                sio_client.disconnect()

        return mimetype, outputs or None

    def __repr__(self) -> str:
        return f'<DokanalyseProcessor> {self.name}'
