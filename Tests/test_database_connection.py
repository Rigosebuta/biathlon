import pytest
from DataProcessing import database_connection as dc
import sqlite3
import json


def test_get_connection():
    """This method tests if the method get_connection() returns a Connection object."""
    conn = dc.get_connection()
    assert isinstance(conn, sqlite3.Connection)


@pytest.fixture
def connection():
    return dc.get_connection()


@pytest.fixture
def cursor():
    return dc.get_connection().cursor()


def test_get_connection_correct_path(cursor):
    """This method tests if the cursor is connected to the database with the correct path."""
    for id_, name, filename in cursor.execute('PRAGMA database_list'):
        if name == 'main' and filename is not None:
            path = ['biathlon', 'Data', 'Biathlon_Data.db']
            filename_ls = filename.split('\\')  # split file name with backslash as separator
            assert path == filename_ls[-3:]
            break


# This test resets some lists of the json file to empty files. If this isn't wanted don't do this test.
def test_create_json_and_db_2(cursor):
    """This method tests if a json file exists in a certain format"""
    dc.create_json_and_db()
    with open("../Data/blacklist.json", 'r') as file:
        blacklist_dict = json.loads(file.read())
        for i in blacklist_dict.values():
            assert isinstance(i, list)  # all elements are lists


def test_get_json_lists():
    """This method tests if the method get_json_lists() returns the data of the json file as lists."""
    json_tuple = dc.get_json_lists()
    for i in json_tuple:
        assert isinstance(i, list)


# This test resets some lists of the json file to empty files. If this isn't wanted don't do this test.
def test_set_json_lists_true_cases():
    """This method tests the method set_json_lists() with a true case."""
    dc.create_json_and_db()
    json_lists = [["Michael"], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
    dc.set_json_lists(json_lists)
    assert json_lists == list(dc.get_json_lists())


@pytest.mark.parametrize("index_error_set_json", [
    [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [[], ["2"]]
])
# This test resets some lists of the json file to empty files. If this isn't wanted don't do this test.
def test_set_json_lists_index_error(index_error_set_json):
    """This method tests the method set_json_lists() with cases which invoke an IndexError"""
    dc.create_json_and_db()
    with pytest.raises(IndexError):
        dc.set_json_lists(index_error_set_json)


@pytest.mark.parametrize("type_error_set_json", [
    None,
    ["not_a_list", [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [1, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [1.5, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [{}, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
])
# This test resets some lists of the json file to empty files. If this isn't wanted don't do this test.
def test_set_json_lists_type_error(type_error_set_json):
    """This method tests the method set_json_lists() with cases which invoke a TypeError"""
    dc.create_json_and_db()
    with pytest.raises(TypeError):
        dc.set_json_lists(type_error_set_json)
