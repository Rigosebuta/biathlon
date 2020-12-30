import pytest
from DataProcessing import biathlete as ba
import sqlite3
import json


def test_get_connection():
    """This method tests if the method get_connection() returns a Connection object"""
    conn = ba.get_connection()
    assert isinstance(conn, sqlite3.Connection)


@pytest.fixture
def connection():
    return ba.get_connection()


@pytest.fixture
def cursor():
    return ba.get_connection().cursor()


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
    ba.create_json_and_db()
    cursor = cursor.execute('SELECT * FROM Athlete')
    names = list(map(lambda x: x[0], cursor.description))
    list_names = ['name', 'birthdate', 'country', 'languages', 'hobbies', 'profession', 'family', 'skis',
                  'rifle', 'ammunition', 'racesuit', 'shoes', 'bindings', 'skipoles', 'gloves', 'wax',
                  'goggles', 'size', 'weight']
    assert names == list_names


def test_create_json_and_db_2(cursor):
    """This method tests if a json file exists in a certain format"""
    ba.create_json_and_db()
    with open("../Data/blacklist.json", 'r') as file:
        blacklist_dict = json.loads(file.read())
        for i in blacklist_dict.values():
            assert isinstance(i, list)
            assert not i


def test_get_json_lists():
    """This method tests if the method get_json_lists returns the data of the json file in lists"""
    json_tuple = ba.get_json_lists()
    for i in json_tuple:
        assert isinstance(i, list)


def test_set_json_lists_true_cases():
    """This method tests the method set_json_lists() with a true case"""
    ba.create_json_and_db()  # empty dictionary
    json_lists = [["Michael"], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
    ba.set_json_lists(json_lists)
    assert json_lists == list(ba.get_json_lists())


def test_set_json_lists_index_error():
    """This method tests the method set_json_lists() with a case which invokes an IndexError"""
    ba.create_json_and_db()  # empty dictionary
    with pytest.raises(IndexError):
        ba.set_json_lists([[], ["2"]])


def test_set_json_lists_type_error():
    """This method tests the method set_json_lists() with a case which invokes a TypeError"""
    ba.create_json_and_db()  # empty dictionary
    with pytest.raises(TypeError):
        ba.set_json_lists(["not_a_list", [], [], [], [], [], [], [], [], [], [], [], [], [], [], []])


def test_create_athlete():
    """This method tests the method create_athlete() with a case which invokes a TypeError"""
    with pytest.raises(TypeError):
        ba.create_athlete(1)


def test_update_athlete_db():
    """This method tests the method update_athlete_db() with a case which invokes a TypeError"""
    with pytest.raises(TypeError):
        ba.update_athlete_db(1)
