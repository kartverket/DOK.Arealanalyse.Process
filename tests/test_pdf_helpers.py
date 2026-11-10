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
