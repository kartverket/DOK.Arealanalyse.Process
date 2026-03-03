from typing import Any, Dict, Tuple
import asyncio
import aiohttp
from osgeo import ogr, osr, gdal
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .services import analyses
from .caching.xsd import cache_base_xml_schemas
from .utils.helpers.request import request_is_valid
from .utils.http_context import set_session, reset_session
from .utils.socket_io import get_client
from .utils.logger import setup as setup_logger
from .utils.correlation import set_correlation_id, clear_correlation_id
from .utils.async_executor import exec_async

gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

setup_logger()
cache_base_xml_schemas()

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
            'includeFactSheet': True,
            'createBinaries': True,
            'correlationId': '42054ecd-90eb-4b97-a271-07281a78d98f'
        }
    }
}


class DokanalyseProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: Dict[str, Any], outputs=None) -> Tuple[str, Dict[str, Any] | None]:
        set_correlation_id(data.get('correlationId'))

        if not request_is_valid(data):
            raise ProcessorExecuteError('Invalid payload')

        outputs = exec_async(self._run_analyses(data))

        return 'application/json', outputs

    async def _run_analyses(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sio_client = get_client()

        timeout = aiohttp.ClientTimeout(total=60)

        connector = aiohttp.TCPConnector(
            limit=100, limit_per_host=10, ttl_dns_cache=300)

        async with asyncio.Semaphore(30):
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                token = set_session(session)

                try:
                    return await analyses.run(data, sio_client)
                finally:
                    reset_session(token)
                    clear_correlation_id()

                    if sio_client:
                        sio_client.disconnect()

    def __repr__(self) -> str:
        return f'<DokanalyseProcessor> {self.name}'
