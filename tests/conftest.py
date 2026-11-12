import os
import tempfile
from pathlib import Path

# dokanalyse.constants reads these at import time, so they must be set
# before any dokanalyse module is imported.
_app_files_dir = tempfile.mkdtemp(prefix='dokanalyse-test-')
os.environ.setdefault('DOKANALYSE_APP_FILES_DIR', _app_files_dir)
os.environ.setdefault('DOKANALYSE_DATASETS_CONFIG_DIR',
                      str(Path(__file__).parent.joinpath('fixtures', 'config')))

import json  # noqa: E402
from typing import TYPE_CHECKING  # noqa: E402
from uuid import UUID  # noqa: E402
import pytest  # noqa: E402
from osgeo import ogr, osr  # noqa: E402

if TYPE_CHECKING:
    from dokanalyse.models.config import DatasetConfig

ogr.UseExceptions()
osr.UseExceptions()

FIXTURES_DIR = Path(__file__).parent.joinpath('fixtures')

# Small square in Oslo, EPSG:25833 (approx. 100 x 100 m)
POLYGON_25833 = {
    'type': 'Polygon',
    'coordinates': [[
        [262400.0, 6649000.0],
        [262500.0, 6649000.0],
        [262500.0, 6649100.0],
        [262400.0, 6649100.0],
        [262400.0, 6649000.0]
    ]],
    'crs': {
        'type': 'name',
        'properties': {'name': 'urn:ogc:def:crs:EPSG::25833'}
    }
}

# Small square in Oslo, WGS84 lon/lat without crs member
POLYGON_WGS84 = {
    'type': 'Polygon',
    'coordinates': [[
        [10.75, 59.91],
        [10.76, 59.91],
        [10.76, 59.92],
        [10.75, 59.92],
        [10.75, 59.91]
    ]]
}


@pytest.fixture
def polygon_25833() -> dict:
    return json.loads(json.dumps(POLYGON_25833))


@pytest.fixture
def polygon_wgs84() -> dict:
    return json.loads(json.dumps(POLYGON_WGS84))


@pytest.fixture
def geometry_25833() -> ogr.Geometry:
    from dokanalyse.utils.helpers.geometry import create_input_geometry
    return create_input_geometry(json.loads(json.dumps(POLYGON_25833)))


def read_fixture(name: str) -> bytes:
    return FIXTURES_DIR.joinpath(name).read_bytes()


async def load_dataset_config(config_id: UUID) -> 'DatasetConfig':
    from dokanalyse.services.config import get_dataset_config

    config = await get_dataset_config(config_id)
    assert config is not None, f'No dataset config with config_id {config_id}'

    return config
