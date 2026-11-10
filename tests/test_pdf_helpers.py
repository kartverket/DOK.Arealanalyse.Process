import importlib.util
from pathlib import Path
import pytest

_TEMPLATES_DIR = Path(__file__).parents[1].joinpath('src', 'dokanalyse', 'pdf_templates')


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(f'test_{path.parent.name}_{path.stem}', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def helpers():
    return _load(_TEMPLATES_DIR.joinpath('functions', 'helpers.py'))


@pytest.fixture(scope='module')
def filters():
    return _load(_TEMPLATES_DIR.joinpath('filters', '__init__.py'))


def _result(**kwargs):
    return {
        'title': 'Flomsone', 'resultStatus': 'HIT-RED', 'hitArea': 50.0, 'inputGeometryArea': 200.0,
        'distanceToObject': 0, 'themes': ['Samfunnssikkerhet'],
        'runOnDataset': {'title': 'Flomsoner', 'datasetId': 'x'}
    } | kwargs


def test_result_title(helpers):
    assert helpers.get_result_title(_result()) == 'Området er i konflikt med «Flomsoner»<div>(Flomsone)</div>'


def test_hit_area_and_percent(helpers):
    assert helpers.get_hit_area(_result()) == '50 m²'
    assert helpers.get_hit_area_percent(_result()) == '25 %'


def test_distance(helpers):
    assert helpers.get_distance(_result(distanceToObject=1500)) == '1\xa0500 m'
    assert helpers.get_distance(_result(distanceToObject=99999)) == '> 20\xa0000 m'


def test_markdown_filter(filters):
    assert filters.markdown('**fet**') == '<p><strong>fet</strong></p>'
    assert filters.markdown(None) == ''


@pytest.fixture(scope='module')
def charts():
    return _load(_TEMPLATES_DIR.joinpath('functions', 'charts.py'))


def test_result_title_without_dataset_metadata(helpers):
    assert helpers.get_result_title(_result(runOnDataset=None)) == 'Området er i konflikt med Flomsone'


def test_results_sorted_by_hit_area(helpers):
    response = {'resultList': [
        _result(title='a', hitArea=10.0), _result(title='b', hitArea=None, themes=[]),
        _result(title='c', hitArea=30.0)]}

    titles = [r['title'] for r in helpers.get_results_by_result_status(response, 'HIT-RED')]

    assert titles == ['c', 'a', 'b']


def test_not_relevant_titles_without_metadata(helpers):
    response = {'resultList': [
        _result(title='B', resultStatus='NOT-RELEVANT', runOnDataset=None),
        _result(title='x', resultStatus='NOT-RELEVANT', runOnDataset={'title': 'A'})]}

    assert helpers.get_not_relevant_dataset_titles(response) == ['A', 'B']


def test_hit_area_percent_without_input_area(helpers):
    assert helpers.get_hit_area_percent(_result(inputGeometryArea=0)) == ''


def test_format_datetime(filters):
    assert filters.format_datetime('2024-05-01T12:00:00') == '01.05.2024'
    assert filters.format_datetime('2024-05-01T12:00:00.123+02:00') == '01.05.2024'
    assert filters.format_datetime(None) == ''


def test_format_number_zero(filters):
    assert filters.format_number(0) == '0'


def test_pie_chart(charts):
    fact_list = [
        {'runOnDataset': None, 'data': []},
        {'runOnDataset': {'datasetId': '166382b4-82d6-4ea9-a68e-6fd0c87bf788'},
         'data': {'areaTypes': [{'areaType': 'Skog', 'area': 100.4}, {'areaType': 'Ny arealtype', 'area': 50},
                                {'areaType': 'Myr', 'area': 0}]}}]

    svg, options = charts.create_pie_chart(fact_list, 'AREA_TYPES')

    assert svg.startswith('<?xml')
    assert [o['label'] for o in options] == ['Skog', 'Ny arealtype']
    assert options[1]['color'] == '#cccccc'


def test_pie_chart_without_data(charts):
    assert charts.create_pie_chart([], 'BUILDINGS') == ('', None)
