from uuid import UUID
import pytest
from dokanalyse.models import Analysis, ResultStatus
from dokanalyse.services import analysis_runner
from conftest import load_dataset_config

ARCGIS_ID = UUID('22222222-2222-4222-8222-222222222222')

_HIT_FEATURE = {
    'type': 'Feature',
    'properties': {'Naturtype': 'Kalkskog', 'Verdikategori': 'Svært stor verdi'},
    'geometry': {
        'type': 'Polygon',
        'coordinates': [[[262450, 6649050], [262550, 6649050], [262550, 6649150],
                         [262450, 6649150], [262450, 6649050]]]
    }
}

_GUIDANCE = {
    'id': '0d000000-0000-4000-8000-000000000003', 'title': 'Veiledning',
    'description': 'Beskrivelse', 'guidance_text': 'Tekst',
    'guidance_uri': [], 'possible_actions': ['Tiltak']
}


class FakeServices:
    """Replaces all network-dependent functions used by an analysis."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.layer_responses: dict = {}
        self.coverage_values: list = ['kartlagt']
        self.queried_layers: list = []

        async def query_arcgis(url, layer, filter, geometry, epsg, config=None):
            url = str(url)

            if 'naturtyper_nin' in url:
                features = [{'type': 'Feature', 'properties': {'Dekningskartverdi': value},
                             'geometry': _HIT_FEATURE['geometry']} for value in self.coverage_values]
                return 200, {'type': 'FeatureCollection', 'features': features}

            self.queried_layers.append(layer)
            status, features = self.layer_responses.get(layer, (200, []))

            if status != 200:
                return status, None

            return 200, {'type': 'FeatureCollection', 'features': features}

        async def get_guidance_data(id):
            return _GUIDANCE if id else None

        async def no_metadata(*args, **kwargs):
            return None

        async def cartography(*args, **kwargs):
            return 'https://legend.example.com'

        async def codelist(*args, **kwargs):
            return []

        monkeypatch.setattr('dokanalyse.services.query_strategies.query_arcgis', query_arcgis)
        monkeypatch.setattr('dokanalyse.services.coverage.query_arcgis', query_arcgis)
        monkeypatch.setattr(analysis_runner, 'get_guidance_data', get_guidance_data)
        monkeypatch.setattr(analysis_runner, 'get_kartkatalog_metadata', no_metadata)
        monkeypatch.setattr(analysis_runner, 'get_cartography_url', cartography)
        monkeypatch.setattr('dokanalyse.services.quality.dataset_quality.get_dok_status_for_dataset', no_metadata)
        monkeypatch.setattr('dokanalyse.services.quality.coverage_quality.get_codelist', codelist)


@pytest.fixture
def fake_services(monkeypatch) -> FakeServices:
    return FakeServices(monkeypatch)


async def _create_analysis(geometry, buffer: int = 0) -> Analysis:
    config = await load_dataset_config(ARCGIS_ID)
    return Analysis(ARCGIS_ID, config, geometry, 25833, 25833, buffer)


async def test_hit_in_first_layer(fake_services, geometry_25833):
    fake_services.layer_responses['0'] = (200, [_HIT_FEATURE])
    analysis = await _create_analysis(geometry_25833)

    await analysis_runner.run_analysis(analysis, 'Reguleringsplan', True, True)

    assert analysis.result_status == ResultStatus.HIT_RED
    assert analysis.hit_area == 2500
    assert analysis.input_geometry_area == 10_000
    assert analysis.data == [{'Naturtype': 'Kalkskog', 'Verdikategori': 'Svært stor verdi'}]
    assert analysis.raster_result_map == \
        'https://arcgis.example.com/arcgis/services/naturtyper/MapServer/WMSServer?layers=svært_stor_verdi'
    assert analysis.cartography == 'https://legend.example.com'
    assert analysis.description == 'Beskrivelse'
    assert analysis.possible_actions == ['Tiltak']
    assert fake_services.queried_layers == ['0']


async def test_hit_in_second_layer(fake_services, geometry_25833):
    fake_services.layer_responses['1'] = (200, [_HIT_FEATURE])
    analysis = await _create_analysis(geometry_25833)

    await analysis_runner.run_analysis(analysis, '', False, False)

    assert analysis.result_status == ResultStatus.HIT_YELLOW
    assert fake_services.queried_layers[:2] == ['0', '1']


async def test_no_hit_sets_distance(fake_services, geometry_25833):
    analysis = await _create_analysis(geometry_25833)

    await analysis_runner.run_analysis(analysis, '', False, False)

    assert analysis.result_status == ResultStatus.NO_HIT_GREEN
    assert analysis.hit_area is None
    assert 'get distance to nearest object' in analysis.run_algorithm


async def test_service_error_gives_error_status(fake_services, geometry_25833):
    fake_services.layer_responses['0'] = (500, None)
    analysis = await _create_analysis(geometry_25833)

    await analysis_runner.run_analysis(analysis, '', False, False)

    assert analysis.result_status == ResultStatus.ERROR
    assert analysis.status_code == 500


async def test_not_mapped_area_gives_no_hit_yellow(fake_services, geometry_25833):
    fake_services.coverage_values = []
    analysis = await _create_analysis(geometry_25833)

    await analysis_runner.run_analysis(analysis, '', False, True)

    assert analysis.result_status == ResultStatus.NO_HIT_YELLOW
    assert fake_services.queried_layers == ['0']  # only the distance query
    assert 'Området er ikke kartlagt for naturtyper' in analysis.quality_warning


async def test_buffer_is_applied(fake_services, geometry_25833):
    analysis = await _create_analysis(geometry_25833, buffer=10)

    await analysis_runner.run_analysis(analysis, '', False, False)

    assert 'add buffer (10)' in analysis.run_algorithm
    assert analysis.input_geometry_area is not None
    assert analysis.input_geometry_area > 10_000
