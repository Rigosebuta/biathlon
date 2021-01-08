import pytest
from DataProcessing import biathlete as ba


@pytest.mark.parametrize("type_error_create_athlete", [
    None, 1, 0, 1.5, [], {}
])
def test_create_athlete(type_error_create_athlete):
    """This method tests the method create_athlete() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        ba.create_athlete(type_error_create_athlete)


@pytest.mark.parametrize("type_error_update_db", [
    None, 1, 0, 1.5, [1, 2, 3], {}, ["Hi", 1, "Bye"]
])
def test_update_athlete_db_1(type_error_update_db):
    """This method tests the method update_athlete_db() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        ba.update_athlete_db(type_error_update_db)
