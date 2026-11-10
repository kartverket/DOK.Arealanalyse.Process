from dokanalyse.utils.helpers.filter import compile_cql_filter, evaluate_filter


def test_numeric_literals_match_numeric_strings():
    f = compile_cql_filter('gjentaksintervall IN (20, 200, 2100)')

    assert evaluate_filter(f, {'gjentaksintervall': '200'})
    assert evaluate_filter(f, {'gjentaksintervall': 200})
    assert not evaluate_filter(f, {'gjentaksintervall': '1000'})


def test_string_literals_still_match():
    f = compile_cql_filter("kps IN ('20', '40')")

    assert evaluate_filter(f, {'kps': '20'})
    assert not evaluate_filter(f, {'kps': '30'})


def test_string_equality_and_missing_property():
    f = compile_cql_filter("servituttType = 'UN'")

    assert evaluate_filter(f, {'servituttType': 'UN'})
    assert not evaluate_filter(f, {'servituttType': 'UT'})
    assert not evaluate_filter(f, {})


def test_comparison_with_numeric_string():
    f = compile_cql_filter('verdi >= 3')

    assert evaluate_filter(f, {'verdi': '4.5'})
    assert not evaluate_filter(f, {'verdi': '2'})
