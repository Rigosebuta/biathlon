import pytest
from DataProcessing import database_connection as dc
import sqlite3
import json


def test_get_connection():
    """This method tests if the method get_connection() returns a Connection object"""
    conn = dc.get_connection()
    assert isinstance(conn, sqlite3.Connection)


@pytest.fixture
def connection():
    return dc.get_connection()


@pytest.fixture
def cursor():
    return dc.get_connection().cursor()


def test_get_connection_correct_path(cursor):
    """This method tests if the cursor is connected with the database in the correct path"""
    for id_, name, filename in cursor.execute('PRAGMA database_list'):
        if name == 'main' and filename is not None:
            path = ['biathlon', 'Data', 'Biathlon_Data.db']
            filename_ls = filename.split('\\')  # split file name with backslash as separator
            assert path == filename_ls[-3:]
            break


def test_create_json_and_db_1(cursor):
    """This method tests if the columns in the database are named correctly """
    dc.create_json_and_db()
    cursor = cursor.execute('SELECT * FROM Athlete')
    names = list(map(lambda x: x[0], cursor.description))
    list_names = ['name', 'birthdate', 'country', 'languages', 'hobbies', 'profession', 'family', 'skis',
                  'rifle', 'ammunition', 'racesuit', 'shoes', 'bindings', 'skipoles', 'gloves', 'wax',
                  'goggles', 'size', 'weight']
    assert names == list_names


def test_create_json_and_db_2(cursor):
    """This method tests if a json file exists in a certain format"""
    dc.create_json_and_db()
    with open("../Data/blacklist.json", 'r') as file:
        blacklist_dict = json.loads(file.read())
        for i in blacklist_dict.values():
            assert isinstance(i, list)
            assert not i


def test_get_json_lists():
    """This method tests if the method get_json_lists returns the data of the json file in lists"""
    json_tuple = dc.get_json_lists()
    for i in json_tuple:
        assert isinstance(i, list)


def test_set_json_lists_true_cases():
    """This method tests the method set_json_lists() with a true case"""
    dc.create_json_and_db()  # empty dictionary
    json_lists = [["Michael"], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
    dc.set_json_lists(json_lists)
    assert json_lists == list(dc.get_json_lists())


def test_set_json_lists_index_error():
    """This method tests the method set_json_lists() with a case which invokes an IndexError"""
    dc.create_json_and_db()  # empty dictionary
    with pytest.raises(IndexError):
        dc.set_json_lists([[], ["2"]])


@pytest.mark.parametrize("type_error_set_json", [
    None,
    ["not_a_list", [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [1, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [1.5, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
    [{}, [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
])
def test_set_json_lists_type_error(type_error_set_json):
    """This method tests the method set_json_lists() with cases which invoke a TypeError"""
    dc.create_json_and_db()  # empty dictionary
    with pytest.raises(TypeError):
        dc.set_json_lists(type_error_set_json)
