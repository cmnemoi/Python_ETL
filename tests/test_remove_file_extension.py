from etl.util import remove_file_extension


def test_remove_file_extension():
    assert remove_file_extension("file.csv") == "file"
    assert remove_file_extension("file.json") == "file"
    assert remove_file_extension("file") == "file"
