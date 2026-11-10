from uuid import UUID
import pytest
from dokanalyse.services.config import (
    get_dataset_configs, get_dataset_config, get_quality_indicator_configs)
from dokanalyse.services.dataset import get_dataset_type
from dokanalyse.models.config import DatasetConfig, QualityIndicatorType
from pydantic import ValidationError

WFS_ID = UUID('11111111-1111-4111-8111-111111111111')
ARCGIS_ID = UUID('22222222-2222-4222-8222-222222222222')
OGC_API_ID = UUID('33333333-3333-4333-8333-333333333333')


async def test_loads_enabled_dataset_configs():
    configs = await get_dataset_configs()
    ids = {config.config_id for config in configs}

    assert ids == {WFS_ID, ARCGIS_ID, OGC_API_ID}


async def test_get_dataset_config_and_type():
    assert get_dataset_type(await get_dataset_config(WFS_ID)) == 'wfs'
    assert get_dataset_type(await get_dataset_config(ARCGIS_ID)) == 'arcgis'
    assert get_dataset_type(await get_dataset_config(OGC_API_ID)) == 'ogc_api'
    assert await get_dataset_config(UUID('99999999-9999-4999-8999-999999999999')) is None


async def test_wfs_layer_filter_is_compiled():
    config = await get_dataset_config(WFS_ID)

    assert config.layers[0].filter_func is not None


async def test_quality_indicators_include_common_indicators():
    indicators = await get_quality_indicator_configs(WFS_ID)
    types = sorted(indicator.type for indicator in indicators)

    assert types == [QualityIndicatorType.DATASET, QualityIndicatorType.DATASET, QualityIndicatorType.OBJECT]
    assert all(indicator.input_filter_func for indicator in indicators
               if indicator.type == QualityIndicatorType.DATASET)


def test_dataset_config_requires_service():
    with pytest.raises(ValidationError):
        DatasetConfig(config_id=WFS_ID, name='x', wms='https://example.com', themes=[])


def test_layer_rejects_invalid_result_status():
    with pytest.raises(ValidationError):
        DatasetConfig(config_id=WFS_ID, name='x', wfs='https://example.com', wms='https://example.com',
                      themes=[], layers=[{'wfs': 'a', 'wms': [], 'result_status': 'ERROR'}])
