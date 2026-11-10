from dokanalyse.models.config import CoverageGeoJson
from dokanalyse.services.coverage import get_values_from_geofile
from conftest import FIXTURES_DIR


async def test_geofile_in_wgs84(geometry_25833):
    config = CoverageGeoJson.model_validate({
        'url': FIXTURES_DIR.joinpath('dekning_wgs84.geojson').as_uri(),
        'property': 'dekningsstatus', 'properties': ['dekningsstatus']})

    values, hit_area_percent, data = await get_values_from_geofile(config, geometry_25833, 25833)

    assert values == ['ikkeKartlagt']
    assert hit_area_percent == 100
    assert data == [{'dekningsstatus': 'ikkeKartlagt'}]
