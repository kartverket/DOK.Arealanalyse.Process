import asyncio
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, cast
import pytest
from PIL import Image, UnidentifiedImageError
from owslib.wmts import WebMapTileService
from dokanalyse.services.map_image import _generate_map_image, basemap, wms
from dokanalyse.services.map_image.spec import MapSpec, get_extent

_ORIGIN = 20037508.3427892

_MATRICES = ''.join(f'''
<TileMatrix><ows:Identifier>{z}</ows:Identifier>
<ScaleDenominator>{559082264.0287178 / 2**z}</ScaleDenominator>
<TopLeftCorner>{-_ORIGIN} {_ORIGIN}</TopLeftCorner>
<TileWidth>256</TileWidth><TileHeight>256</TileHeight>
<MatrixWidth>{2**z}</MatrixWidth><MatrixHeight>{2**z}</MatrixHeight></TileMatrix>''' for z in range(19))

_LAYERS = ''.join(f'''
<Layer><ows:Identifier>{name}</ows:Identifier>
<Style isDefault="true"><ows:Identifier>default</ows:Identifier></Style><Format>image/png</Format>
<TileMatrixSetLink><TileMatrixSet>utm33n</TileMatrixSet></TileMatrixSetLink>
<TileMatrixSetLink><TileMatrixSet>webmercator</TileMatrixSet></TileMatrixSetLink>
<ResourceURL format="image/png" resourceType="tile"
  template="https://wmts.example.com/{name}/default/{{TileMatrixSet}}/{{TileMatrix}}/{{TileRow}}/{{TileCol}}.png"/>
</Layer>''' for name in ('topo', 'topograatone'))

_CAPABILITIES = f'''<?xml version="1.0"?>
<Capabilities xmlns="http://www.opengis.net/wmts/1.0" xmlns:ows="http://www.opengis.net/ows/1.1" version="1.0.0">
<Contents>{_LAYERS}
<TileMatrixSet><ows:Identifier>utm33n</ows:Identifier>
<ows:SupportedCRS>urn:ogc:def:crs:EPSG::25833</ows:SupportedCRS>{_MATRICES}</TileMatrixSet>
<TileMatrixSet><ows:Identifier>webmercator</ows:Identifier>
<ows:SupportedCRS>urn:ogc:def:crs:EPSG::3857</ows:SupportedCRS>{_MATRICES}</TileMatrixSet>
</Contents></Capabilities>'''.encode()

# Approx. 300 x 200 m in Trondheim, EPSG:3857
_GEOMETRY = 'POLYGON ((1157000 9200000,1157300 9200000,1157300 9200200,1157000 9200200,1157000 9200000))'

_TILE_COLOR = (200, 220, 200, 255)


def _png(size: tuple[int, int], color: tuple[int, int, int, int]) -> bytes:
    buffer = BytesIO()
    Image.new('RGBA', size, color).save(buffer, 'PNG')

    return buffer.getvalue()


class FakeResponse:
    def __init__(self, body: bytes, content_type: str, status: int = 200):
        self.body = body
        self.content_type = content_type
        self.status = status
        self.ok = status < 400

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def read(self):
        return self.body


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests: List[tuple[str, dict]] = []

    def get(self, url, params=None, **kwargs):
        self.requests.append((url, params or {}))
        return self.response


@pytest.fixture
def wmts() -> WebMapTileService:
    return WebMapTileService('https://wmts.example.com/WMTSCapabilities.xml', xml=_CAPABILITIES)


@pytest.fixture
def fake_services(monkeypatch, tmp_path: Path) -> List[str]:
    """Capabilities from a file, tiles and WMS images without network calls.
    Returns the URLs of the downloaded tiles."""
    downloaded_urls: List[str] = []
    capabilities_path = tmp_path.joinpath('wmts-capabilities.xml')
    capabilities_path.write_bytes(_CAPABILITIES)

    async def get_capabilities(session):
        return capabilities_path

    async def download_tile(url: str, path: Path):
        downloaded_urls.append(url)
        await asyncio.sleep(0.01)
        basemap._write_tile(path, _png((256, 256), _TILE_COLOR))

    async def fetch_wms_image(url, layers, extent, size):
        return Image.new('RGBA', size, (0, 0, 255, 128))

    monkeypatch.setattr(basemap, '_tiles_cache_dir', tmp_path.joinpath('tiles'))
    monkeypatch.setattr(basemap, '_wmts_service', None)
    monkeypatch.setattr(basemap, 'get_or_create_wmts_capabilities', get_capabilities)
    monkeypatch.setattr(basemap, 'get_session', lambda: None)
    monkeypatch.setattr(basemap, '_download_tile', download_tile)
    monkeypatch.setattr('dokanalyse.services.map_image.fetch_wms_image', fetch_wms_image)

    return downloaded_urls


def _spec(id: str, buffer: str | None = None) -> MapSpec:
    return MapSpec(
        id=id, name=id, geometry=_GEOMETRY, buffer=buffer,
        extent=get_extent(_GEOMETRY, buffer, (1280, 720)),
        grayscale=True, wms_url='https://wms.example.com/wms', wms_layers=['lag'])


def test_extent_has_image_aspect_ratio_and_margin():
    minx, miny, maxx, maxy = get_extent(_GEOMETRY, None, (1280, 720))

    assert (maxx - minx) / (maxy - miny) == pytest.approx(1280 / 720)
    assert maxx - minx == pytest.approx(200 * 1280 / 720 * 1.1)
    assert (minx + maxx) / 2 == pytest.approx(1157150)


def test_uses_web_mercator_matrix_set_and_matching_resolution(wmts):
    matrix_set = basemap._get_web_mercator_matrix_set(wmts, 'topo')
    matrices = matrix_set.tilematrix.values()

    assert matrix_set.identifier == 'webmercator'
    # Zoom 18 is ~0.597 m/px, the finest one available
    assert basemap._choose_tile_matrix(matrices, 0.5).identifier == '18'
    assert basemap._choose_tile_matrix(matrices, 1.5).identifier == '17'


def test_tile_url_uses_rest_template(wmts):
    url = basemap._get_tile_url(wmts, 'topo', 'webmercator', '12', 1100, 2200)

    assert url == 'https://wmts.example.com/topo/default/webmercator/12/1100/2200.png'


async def test_generates_map_images_with_shared_and_cached_tiles(fake_services):
    specs = [_spec('a'), _spec('b'), _spec('c')]

    results = await asyncio.gather(*(_generate_map_image(spec) for spec in specs))

    for result in results:
        assert result is not None and result[2]

        with Image.open(BytesIO(result[2])) as image:
            assert image.size == (1280, 720)

    # The images share the same tiles, so each tile is downloaded only once
    downloaded = list(fake_services)
    assert downloaded
    assert len(downloaded) == len(set(downloaded))

    await _generate_map_image(_spec('d'))

    assert fake_services == downloaded


async def test_map_image_is_georeferenced(fake_services):
    result = await _generate_map_image(_spec('a'))

    assert result is not None and result[2]

    with Image.open(BytesIO(result[2])) as image:
        pixels = image.convert('RGB')

        def pixel(x: int, y: int) -> Tuple[int, int, int]:
            return cast(Tuple[int, int, int], pixels.getpixel((x, y)))

        minx, _, maxx, _ = get_extent(_GEOMETRY, None, (1280, 720))
        # The left edge of the polygon (x = 1157000), at mid height
        edge_x = round((1157000 - minx) / (maxx - minx) * 1280)

        assert pixel(edge_x, 360) == pytest.approx((211, 51, 51), abs=10)
        # Outside the polygon: the basemap with the semi-transparent WMS image on top
        red, green, blue = pixel(20, 20)
        assert blue > red and blue > 150 and green > 50


async def test_failed_tile_download_fails_only_that_image(fake_services, monkeypatch):
    async def failing_download(url: str, path: Path):
        raise OSError('connection reset')

    monkeypatch.setattr(basemap, '_download_tile', failing_download)

    assert await _generate_map_image(_spec('a')) is None
    assert not basemap._in_flight


async def test_failed_wms_request_fails_only_that_image(fake_services, monkeypatch):
    async def failing_wms(url, layers, extent, size):
        raise ValueError('Expected an image')

    monkeypatch.setattr('dokanalyse.services.map_image.fetch_wms_image', failing_wms)

    assert await _generate_map_image(_spec('a')) is None
    assert await _generate_map_image(MapSpec(
        id='omraade', name='omraade', geometry=_GEOMETRY, buffer=None,
        extent=get_extent(_GEOMETRY, None, (1280, 720)), grayscale=False)) is not None


async def test_wms_request_in_web_mercator(monkeypatch):
    session = FakeSession(FakeResponse(_png((1280, 720), (0, 0, 0, 0)), 'image/png'))
    monkeypatch.setattr(wms, 'get_session', lambda: session)

    image = await wms.fetch_wms_image('https://wms.example.com/wms', ['a', 'b'], (1, 2, 3, 4), (1280, 720))

    assert image.size == (1280, 720)
    url, params = session.requests[0]
    assert url == 'https://wms.example.com/wms'
    assert params['CRS'] == 'EPSG:3857'
    assert params['BBOX'] == '1,2,3,4'
    assert params['LAYERS'] == 'a,b'


@pytest.mark.parametrize('status', [200, 400])
async def test_wms_service_exception_is_reported_without_url(monkeypatch, status: int):
    xml = b'''<?xml version="1.0" encoding="UTF-8"?>
<ServiceExceptionReport version="1.3.0" xmlns="http://www.opengis.net/ogc">
<ServiceException code="LayerNotDefined">
Invalid layer(s) given in the LAYERS parameter.
</ServiceException>
</ServiceExceptionReport>'''
    monkeypatch.setattr(wms, 'get_session', lambda: FakeSession(FakeResponse(xml, 'text/xml', status)))

    with pytest.raises(ValueError) as error:
        await wms.fetch_wms_image('https://wms.example.com/wms', ['lag'], (1, 2, 3, 4), (1280, 720))

    assert str(error.value) == \
        f'Expected an image, got HTTP {status} (text/xml): Invalid layer(s) given in the LAYERS parameter.'


async def test_error_text_falls_back_to_body():
    assert wms._get_error_text(b'<html>\n<body>Not   found</body></html>') == '<html> <body>Not found</body></html>'


async def test_invalid_tile_is_not_cached(monkeypatch, tmp_path: Path):
    path = tmp_path.joinpath('12', '1.png')
    monkeypatch.setattr(basemap, 'get_session', lambda: FakeSession(FakeResponse(b'not a png', 'image/png')))

    with pytest.raises(UnidentifiedImageError):
        await basemap._download_tile('https://wmts.example.com/tile.png', path)

    assert not path.exists()
    assert not list(tmp_path.rglob('*.tmp'))
