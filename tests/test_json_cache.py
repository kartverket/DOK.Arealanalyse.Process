import os
from dokanalyse.caching.json_cache import read_json


def test_reads_once_until_file_changes(tmp_path):
    path = tmp_path.joinpath('data.json')
    path.write_text('[1, 2]')
    calls = []

    def transform(data):
        calls.append(1)
        return set(data)

    assert read_json(path, transform, key='set') == {1, 2}
    assert read_json(path, transform, key='set') == {1, 2}
    assert len(calls) == 1
    assert read_json(path) == [1, 2]

    path.write_text('[1, 2, 3]')
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

    assert read_json(path, transform, key='set') == {1, 2, 3}
    assert len(calls) == 2
