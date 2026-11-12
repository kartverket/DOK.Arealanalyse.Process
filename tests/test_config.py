from uuid import UUID
import pytest
from dokanalyse.services.config import (
    get_dataset_configs, get_dataset_config, get_quality_indicator_configs)
from dokanalyse.models.config import DatasetConfig, QualityIndicatorType
from pydantic import ValidationError
from conftest import load_dataset_config

WFS_ID = UUID('11111111-1111-4111-8111-111111111111')
ARCGIS_ID = UUID('22222222-2222-4222-8222-222222222222')
OGC_API_ID = UUID('33333333-3333-4333-8333-333333333333')


async def test_loads_enabled_dataset_configs():
    configs = await get_dataset_configs()
    ids = {config.config_id for config in configs}

    assert ids == {WFS_ID, ARCGIS_ID, OGC_API_ID}


async def test_get_dataset_config_and_type():
    assert (await load_dataset_config(WFS_ID)).service_type == 'wfs'
    assert (await load_dataset_config(ARCGIS_ID)).service_type == 'arcgis'
    assert (await load_dataset_config(OGC_API_ID)).service_type == 'ogc_api'
    assert await get_dataset_config(UUID('99999999-9999-4999-8999-999999999999')) is None


async def test_wfs_layer_filter_is_compiled():
    config = await load_dataset_config(WFS_ID)

    assert config.layers[0].filter_func is not None


async def test_quality_indicators_include_common_indicators():
    indicators = await get_quality_indicator_configs(WFS_ID)
    types = sorted(indicator.type for indicator in indicators)

    assert types == [QualityIndicatorType.DATASET, QualityIndicatorType.DATASET, QualityIndicatorType.OBJECT]
    assert all(indicator.input_filter_func for indicator in indicators
               if indicator.type == QualityIndicatorType.DATASET)


def test_dataset_config_requires_service():
    with pytest.raises(ValidationError):
        DatasetConfig.model_validate({'config_id': WFS_ID, 'name': 'x', 'wms': 'https://example.com', 'themes': []})


def test_dataset_config_rejects_unknown_service_type():
    with pytest.raises(ValidationError):
        DatasetConfig.model_validate({
            'config_id': WFS_ID, 'name': 'x', 'features': {'type': 'wms', 'url': 'https://example.com'},
            'wms': 'https://example.com', 'themes': []})


def test_layer_rejects_invalid_result_status():
    with pytest.raises(ValidationError):
        DatasetConfig.model_validate({
            'config_id': WFS_ID, 'name': 'x', 'features': {'type': 'wfs', 'url': 'https://example.com'},
            'wms': 'https://example.com', 'themes': [],
            'layers': [{'feature': 'a', 'wms': [], 'result_status': 'ERROR'}]})


@pytest.fixture
def config_dir(tmp_path, monkeypatch):
    from dokanalyse.services import config as config_module

    monkeypatch.setattr(config_module, 'get_config_file_paths', lambda: list(tmp_path.glob('*.yml')))
    monkeypatch.setattr(config_module, '_snapshot', None)
    monkeypatch.setattr(config_module, '_last_checked', 0)

    yield tmp_path

    config_module._snapshot = None


_DATASET_YML = '''
type: dataset
config_id: {config_id}
name: {name}
features:
  type: wfs
  url: https://wfs.example.com
wms: https://wms.example.com
layers:
  - feature: Layer
    wms: [layer]
    result_status: HIT-RED
geom_field: område
themes: [Natur]
'''


async def test_invalid_files_do_not_prevent_loading(config_dir):
    config_dir.joinpath('a.yml').write_text(_DATASET_YML.format(config_id=WFS_ID, name='a'))
    config_dir.joinpath('b.yml').write_text('type: dataset\nconfig_id: [unclosed')
    config_dir.joinpath('c.yml').write_text(_DATASET_YML.format(config_id=WFS_ID, name='duplicate'))
    config_dir.joinpath('d.yml').write_text(
        _DATASET_YML.format(config_id=ARCGIS_ID, name='bad_filter') + '    filter: "this is not cql ("\n')

    configs = await get_dataset_configs()

    assert [config.name for config in configs] == ['a']


async def test_reloads_when_files_change(config_dir, monkeypatch):
    from dokanalyse.services import config as config_module

    config_dir.joinpath('a.yml').write_text(_DATASET_YML.format(config_id=WFS_ID, name='a'))
    assert len(await get_dataset_configs()) == 1

    config_dir.joinpath('b.yml').write_text(_DATASET_YML.format(config_id=ARCGIS_ID, name='b'))
    assert len(await get_dataset_configs()) == 1  # within the check interval

    monkeypatch.setattr(config_module, '_last_checked', 0)
    assert len(await get_dataset_configs()) == 2


def test_quality_config_allows_only_one_coverage_indicator():
    from dokanalyse.models.config import QualityConfig

    coverage = {'type': 'coverage', 'quality_dimension_id': 'x', 'quality_dimension_name': 'X',
                'features': {'type': 'arcgis', 'url': 'https://example.com', 'layer': '0', 'property': 'p'}}

    with pytest.raises(ValidationError):
        QualityConfig.model_validate({'indicators': [coverage, coverage]})

    QualityConfig.model_validate({'indicators': [coverage, coverage | {'disabled': True}]})


async def test_not_implemented_dataset_configs(monkeypatch):
    from dokanalyse.services import config as config_module
    from dokanalyse.services.config import get_not_implemented_dataset_configs

    async def dok_status():
        return [
            {'dataset_id': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 'name': 'flomsoner', 'theme': 'Samfunnssikkerhet'},
            {'dataset_id': 'dddddddd-dddd-4ddd-8ddd-dddddddddddd', 'name': 'stoy-sone', 'theme': 'Forurensning'}
        ]

    monkeypatch.setattr(config_module, 'get_dok_status', dok_status)

    configs = await get_not_implemented_dataset_configs()
    again = await get_not_implemented_dataset_configs()

    assert [config.name for config in configs] == ['stoy_sone']
    assert configs[0].themes == ['Forurensning']
    assert configs[0].config_id == again[0].config_id
