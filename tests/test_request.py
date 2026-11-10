from dokanalyse.utils.helpers.request import get_request_validation_error


def test_valid_polygon(polygon_25833, polygon_wgs84):
    assert get_request_validation_error({'inputGeometry': polygon_25833}) is None
    assert get_request_validation_error({'inputGeometry': polygon_wgs84, 'requestedBuffer': 10,
                                         'context': 'Byggesak', 'includeGuidance': True}) is None


def test_valid_multipolygon(polygon_25833):
    multi = {'type': 'MultiPolygon', 'coordinates': [polygon_25833['coordinates']]}

    assert get_request_validation_error({'inputGeometry': multi}) is None


def test_invalid_requests(polygon_25833):
    assert get_request_validation_error({}) is not None
    assert get_request_validation_error({'inputGeometry': {'type': 'Point', 'coordinates': [1, 2]}}) is not None
    assert get_request_validation_error({'inputGeometry': {'type': 'Polygon', 'coordinates': [[[1, 2]]]}}) is not None

    error = get_request_validation_error({'inputGeometry': polygon_25833, 'requestedBuffer': 'ti'})
    assert error is not None and error.startswith('requestedBuffer:')
