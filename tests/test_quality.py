from dokanalyse.models.config import QualityIndicator
from dokanalyse.services.quality import get_threshold_values
from dokanalyse.services.quality.object_quality import get_object_quality
from dokanalyse.services.quality.dataset_quality import _get_dataset_quality_warning_text
from dokanalyse.services.quality.coverage_quality import (
    _has_coverage, _is_relevant, _get_warning_text)
from dokanalyse.utils.helpers.filter import compile_cql_filter


def _indicator(**kwargs) -> QualityIndicator:
    defaults = {
        'type': 'object',
        'quality_dimension_id': 'stedfestingsnøyaktighet',
        'quality_dimension_name': 'Stedfestingsnøyaktighet',
        'quality_warning_text': 'Usikker stedfesting'
    }

    return QualityIndicator.model_validate(defaults | kwargs)


def test_threshold_values():
    assert get_threshold_values(_indicator(warning_threshold='1 OR 2')) == [1, 2]
    assert get_threshold_values(_indicator(warning_threshold='Mindre god')) == ['Mindre god']
    assert get_threshold_values(_indicator()) == []


def test_object_quality_warns_when_threshold_value_present():
    indicator = _indicator(property='klasse', warning_threshold='Mindre god')
    data = [{'klasse': 'God'}, {'klasse': 'Mindre god'}, {'klasse': 'God'}]

    measurements, warnings = get_object_quality([indicator], data)

    assert [m.value for m in measurements] == ['God', 'Mindre god']
    assert warnings == ['Usikker stedfesting']


def test_object_quality_no_warning():
    indicator = _indicator(property='klasse', warning_threshold='Mindre god')

    _, warnings = get_object_quality([indicator], [{'klasse': 'God'}])

    assert warnings == []


def test_dataset_quality_warning_respects_input_filter():
    input_filter = "context = 'Byggesak'"
    indicator = _indicator(type='dataset', warning_threshold='1 OR 2', input_filter=input_filter)
    indicator.input_filter_func = compile_cql_filter(input_filter)

    assert _get_dataset_quality_warning_text(1, indicator, {'context': 'Byggesak'}) == 'Usikker stedfesting'
    assert _get_dataset_quality_warning_text(3, indicator, {'context': 'Byggesak'}) is None
    assert _get_dataset_quality_warning_text(1, indicator, {'context': 'Kommuneplan'}) is None


def test_has_coverage():
    assert _has_coverage(['kartlagt'], None)
    assert not _has_coverage([], None)
    assert not _has_coverage(['ikkeKartlagt'], 'ikkeKartlagt')
    assert _has_coverage(['ikkeKartlagt', 'kartlagt'], 'ikkeKartlagt')


def test_is_relevant():
    assert _is_relevant([])
    assert not _is_relevant(['ikkeRelevant'])
    assert _is_relevant(['ikkeRelevant', 'kartlagt'])


def test_coverage_warning_text_with_partial_hit_area():
    indicator = _indicator(type='coverage', warning_threshold='ikkeKartlagt',
                           quality_warning_text='Området er ikke kartlagt',
                           features={'type': 'arcgis', 'url': 'https://example.com', 'layer': '0', 'property': 'p'})

    assert _get_warning_text(indicator, ['ikkeKartlagt'], 40.5) == '40,5 % av området er ikke kartlagt'
    assert _get_warning_text(indicator, ['ikkeKartlagt'], 100) == 'Området er ikke kartlagt'
    assert _get_warning_text(indicator, ['kartlagt'], 0) is None
