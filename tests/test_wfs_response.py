from uuid import UUID
from dokanalyse.services.config import get_dataset_config
from dokanalyse.services.wfs_response import WfsResponseParser, CoverageWfsResponseParser
from dokanalyse.models.config import CoverageWfs
from conftest import read_fixture

WFS_ID = UUID('11111111-1111-4111-8111-111111111111')


async def test_parses_configured_properties_and_geometries():
    config = await get_dataset_config(WFS_ID)
    layer = config.layers[0].model_copy(update={'filter_func': None})

    result = WfsResponseParser(config, layer).parse(read_fixture('wfs_flomareal.xml'))

    assert len(result['geometries']) == 3
    assert result['properties'][0] == {'gjentaksintervall': '200', 'nøyaktighetsklasse': 'Mindre god'}
    assert round(result['geometries'][0].GetArea()) == 10_000


def test_coverage_parser_collects_values():
    config = CoverageWfs(url='https://wfs.example.com', layer='FlomAreal', geom_field='område',
                         property='nøyaktighetsklasse', properties=['gjentaksintervall'])

    values, geoms, data = CoverageWfsResponseParser(config).parse(read_fixture('wfs_flomareal.xml'))

    assert values == ['Mindre god', 'God', 'God']
    assert geoms == []
    assert data[0] == {'gjentaksintervall': '200'}
