import pytest
from dokanalyse.services import analyses
from conftest import read_fixture
from test_analysis_flow import FakeServices


@pytest.fixture
def fake_request_services(monkeypatch) -> FakeServices:
    services = FakeServices(monkeypatch)

    async def municipality(geometry, epsg):
        return '0301', 'Oslo'

    async def query_wfs(url, layer, geom_field, geometry, epsg, config=None):
        return 200, read_fixture('wfs_flomareal.xml')

    async def query_ogc_api(*args, **kwargs):
        raise RuntimeError('Unexpected failure')

    async def no_buildings(*args, **kwargs):
        return 500, None

    async def not_implemented():
        return []

    monkeypatch.setattr(analyses, 'get_municipality', municipality)
    monkeypatch.setattr(analyses, 'get_not_implemented_dataset_configs', not_implemented)
    monkeypatch.setattr('dokanalyse.services.query_strategies.query_wfs', query_wfs)
    monkeypatch.setattr('dokanalyse.services.query_strategies.query_ogc_api', query_ogc_api)
    monkeypatch.setattr('dokanalyse.services.fact_sheet.buildings.query_wfs', no_buildings)
    monkeypatch.setattr('dokanalyse.services.fact_sheet.buildings.get_kartkatalog_metadata',
                        lambda *args: _none())

    return services


async def _none():
    return None


async def test_failing_dataset_does_not_fail_request(fake_request_services, polygon_25833):
    data = {
        'inputGeometry': polygon_25833,
        'includeFilterChosenDOK': False,
        'includeGuidance': True,
        'includeQualityMeasurement': True,
        'createBinaries': False
    }

    response = await analyses.run(data, None)

    statuses = {result['themes'][0]: result['resultStatus'] for result in response['resultList']}

    assert response['municipalityNumber'] == '0301'
    assert response['inputGeometryArea'] == 10_000
    assert statuses['Samfunnssikkerhet'] == 'HIT-RED'
    assert statuses['Kulturminner'] == 'ERROR'
    assert statuses['Natur'] == 'NO-HIT-GREEN'
    assert response['report'] is None
