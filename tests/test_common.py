from pathlib import Path
from dokanalyse.utils.helpers.common import (
    file_url_to_path, objectify_properties, parse_date_string, parse_string)
from dokanalyse.utils.helpers.gdal import normalize_object


def test_file_url_to_path():
    assert file_url_to_path('file:///mnt/data/my%20file.gpkg') == Path('/mnt/data/my file.gpkg')
    assert file_url_to_path('https://example.com/file.gpkg') is None


def test_objectify_properties():
    result = objectify_properties({'a.b': 1, 'a.c': 2, 'd': 3})

    assert result == {'a': {'b': 1, 'c': 2}, 'd': 3}


def test_parse_string_numbers():
    assert parse_string('42') == 42
    assert parse_string('4.2') == 4.2
    assert parse_string('Mindre god') == 'Mindre god'
    assert parse_string('-5') == -5
    assert parse_string('true') is True
    assert parse_string('False') is False
    assert parse_string('nan-verdi') == 'nan-verdi'


def test_parse_date_string():
    assert parse_date_string('2024-05-01T12:00:00').year == 2024
    assert parse_date_string('not a date') is None


def test_normalize_object_dates_and_json():
    result = normalize_object({
        'date': '2024/05/01',
        'datetime': '2024/05/01 10:20:30+02',
        'json': '{"a": 1}',
        'text': 'tekst',
        'number': 5
    })

    assert result['date'] == '2024-05-01'
    assert result['json'] == {'a': 1}
    assert result['text'] == 'tekst'
    assert result['number'] == 5
