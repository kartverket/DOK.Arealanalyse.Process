import pytest
from pydantic import ValidationError
from dokanalyse.models import AnalysisInput
from dokanalyse.utils.helpers.request import get_validation_error_message


def _get_error(data) -> str | None:
    try:
        AnalysisInput.model_validate(data)
    except ValidationError as err:
        return get_validation_error_message(err)

    return None


def test_valid_polygon(polygon_25833, polygon_wgs84):
    assert _get_error({'inputGeometry': polygon_25833}) is None
    assert _get_error({'inputGeometry': polygon_wgs84, 'requestedBuffer': 10,
                       'context': 'Byggesak', 'includeGuidance': True}) is None


def test_valid_multipolygon(polygon_25833):
    multi = {'type': 'MultiPolygon', 'coordinates': [polygon_25833['coordinates']]}

    assert _get_error({'inputGeometry': multi}) is None


def test_defaults(polygon_25833):
    analysis_input = AnalysisInput.model_validate({'inputGeometry': polygon_25833, 'unknown': 1})

    assert analysis_input.requested_buffer == 0
    assert analysis_input.theme is None
    assert analysis_input.context is None
    assert not analysis_input.include_guidance
    assert not analysis_input.include_quality_measurement
    assert analysis_input.include_filter_chosen_dok
    assert analysis_input.include_facts
    assert analysis_input.create_binaries
    assert analysis_input.correlation_id is None


def test_camel_case_input(polygon_25833):
    analysis_input = AnalysisInput.model_validate({
        'inputGeometry': polygon_25833,
        'requestedBuffer': 10.0,
        'theme': 'Natur',
        'includeFilterChosenDOK': False,
        'includeFacts': False,
        'createBinaries': False,
        'correlationId': '42054ecd-90eb-4b97-a271-07281a78d98f'
    })

    assert analysis_input.requested_buffer == 10
    assert analysis_input.theme == 'Natur'
    assert not analysis_input.include_filter_chosen_dok
    assert not analysis_input.include_facts
    assert not analysis_input.create_binaries
    assert analysis_input.correlation_id == '42054ecd-90eb-4b97-a271-07281a78d98f'


def test_input_geometry_is_kept_unchanged():
    geojson = {
        'type': 'Polygon',
        'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 0]]],
        'crs': {'type': 'name', 'properties': {'name': 'urn:ogc:def:crs:EPSG::25833'}}
    }

    analysis_input = AnalysisInput.model_validate({'inputGeometry': geojson})

    assert analysis_input.input_geometry == geojson
    assert isinstance(analysis_input.input_geometry['coordinates'][0][1][0], int)


def test_invalid_requests(polygon_25833):
    assert _get_error({}) == 'inputGeometry: Field required'
    assert _get_error({'inputGeometry': {'type': 'Point', 'coordinates': [1, 2]}}) is not None
    assert _get_error({'inputGeometry': {'type': 'Polygon', 'coordinates': [[[1, 2]]]}}) is not None

    error = _get_error({'inputGeometry': polygon_25833, 'requestedBuffer': 'ti'})
    assert error is not None and error.startswith('requestedBuffer:')


@pytest.mark.parametrize('field, value', [
    ('requestedBuffer', 10.5),
    ('requestedBuffer', '10'),
    ('includeGuidance', 'true'),
    ('includeFacts', 1),
    ('theme', 1),
])
def test_values_are_not_coerced(polygon_25833, field, value):
    error = _get_error({'inputGeometry': polygon_25833, field: value})

    assert error is not None and error.startswith(f'{field}:')
