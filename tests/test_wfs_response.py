from uuid import UUID
from dokanalyse.services.wfs_response import WfsResponseParser, CoverageWfsResponseParser
from dokanalyse.models.config import CoverageWfs
from conftest import load_dataset_config, read_fixture

WFS_ID = UUID('11111111-1111-4111-8111-111111111111')


async def test_parses_configured_properties_and_geometries():
    config = await load_dataset_config(WFS_ID)
    layer = config.layers[0].model_copy(update={'filter_func': None})

    result = WfsResponseParser(config, layer).parse(read_fixture('wfs_flomareal.xml'))

    assert len(result['geometries']) == 3
    assert result['properties'][0] == {'gjentaksintervall': '200', 'nøyaktighetsklasse': 'Mindre god'}
    assert round(result['geometries'][0].GetArea()) == 10_000


async def test_layer_filter_matches_numeric_values():
    config = await load_dataset_config(WFS_ID)

    result = WfsResponseParser(config, config.layers[0]).parse(read_fixture('wfs_flomareal.xml'))

    assert [p['gjentaksintervall'] for p in result['properties']] == ['200', '20']


def test_coverage_parser_collects_values():
    config = CoverageWfs.model_validate({
        'type': 'wfs', 'url': 'https://wfs.example.com', 'layer': 'FlomAreal', 'geom_field': 'område',
        'property': 'nøyaktighetsklasse', 'properties': ['gjentaksintervall']})

    values, geoms, data = CoverageWfsResponseParser(config).parse(read_fixture('wfs_flomareal.xml'))

    assert values == ['Mindre god', 'God', 'God']
    assert geoms == []
    assert data[0] == {'gjentaksintervall': '200'}


def test_gml_geometry_in_lat_lon_urn_crs_is_transformed():
    from dokanalyse.services.wfs_response import WfsResponseParserBase

    class Parser(WfsResponseParserBase):
        def parse(self, response):
            return None

    from lxml import etree as ET
    gml = ('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::4326">'
           '<gml:pos>59.91 10.75</gml:pos></gml:Point>')
    geometry = Parser('område', 'x', [], [], None)._create_geometry(ET.fromstring(gml))

    assert geometry is not None
    x, y = geometry.GetX(), geometry.GetY()
    assert 262_000 < x < 264_000 and 6_648_000 < y < 6_651_000
