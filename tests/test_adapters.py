from urllib.parse import urlparse, parse_qs
from dokanalyse.adapters import get_credential, get_auth
from dokanalyse.adapters.ogc_api import _create_request_url
from dokanalyse.models.config.auth import ApiKey, Basic


def test_get_credential_from_env(monkeypatch):
    monkeypatch.setenv('MY_SECRET', 's3cret')

    assert get_credential('${MY_SECRET}') == 's3cret'
    assert get_credential('plain') == 'plain'


def test_get_auth():
    assert get_auth(ApiKey(api_key='key'))['headers'] == {'x-api-key': 'key'}
    assert get_auth(Basic(username='u', password='p'))['auth'].login == 'u'
    assert get_auth(None) == {'auth': None, 'headers': {}}


def test_ogc_api_request_url(geometry_25833):
    url = _create_request_url('https://api.example.com', 'enkeltminner', 'område',
                              geometry_25833, "vernetype = 'AUT'", 25833)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.path == '/collections/enkeltminner/items'
    assert query['filter-lang'] == ['cql2-text']
    assert query['filter-crs'] == ['http://www.opengis.net/def/crs/EPSG/0/25833']
    assert query['filter'][0].startswith('S_INTERSECTS(område,POLYGON ((262400 6649000')
    assert query['filter'][0].endswith(" AND vernetype = 'AUT'")
