"""This module represents the interface to the database and the json file"""
import json
import sqlite3
from DataProcessing import extracting_data as ed
import pandas as pd


def create_json_and_db():
    """This method creates a json file and a database.

        This method should only be invoked once at the beginning. If this method is invoked at a later
        date then all json lists in the json file reset to empty lists.
    """
    # create an Athlete table if it doesn't already exist
    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
        cursor = connection.cursor()

        athlete_sql = """
                    CREATE TABLE IF NOT EXISTS ATHLETE (
                    name VARCHAR(40) NOT NULL, 
                    birthdate VARCHAR(50) NOT NULL, 
                    country VARCHAR(3) NOT NULL,
                    languages VARCHAR(400),
                    hobbies VARCHAR(500),
                    profession VARCHAR(100),
                    family VARCHAR(100),
                    skis VARCHAR(20),
                    rifle VARCHAR(20),
                    ammunition VARCHAR(20),
                    racesuit VARCHAR(20),
                    shoes VARCHAR(20),
                    bindings VARCHAR(20),
                    skipoles VARCHAR(20),
                    gloves VARCHAR(20),
                    wax VARCHAR(20),
                    goggles VARCHAR(20),
                    size INTEGER,
                    weight INTEGER,
                    gender INTEGER,
                    birthplace VARCHAR(100),
                    residence VARCHAR(100),
                    PRIMARY KEY(name, birthdate, country)
                    );"""
        cursor.execute(athlete_sql)
        connection.commit()

        race_sql = """
                    CREATE TABLE IF NOT EXISTS RACE (
                    place VARCHAR(20) NOT NULL,
                    year VARCHAR(20) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    age VARCHAR(20) NOT NULL,
                    gender VARCHAR(20) NOT NULL,
                    race_len_km VARCHAR(20),
                    number_of_entries INTEGER,
                    did_not_start INTEGER,
                    did_not_finish INTEGER,
                    lapped INTEGER,
                    disqualified INTEGER,
                    disqualified_for_unsportsmanlike_behaviour INTEGER,
                    ranked INTEGER,
                    PRIMARY KEY(place, year, type, age, gender)
                    );"""
        cursor.execute(race_sql)
        connection.commit()

        weather_sql = """
                    CREATE TABLE IF NOT EXISTS WEATHER (
                    thirty_min_before_start VARCHAR(20),
                    at_start_time VARCHAR(20),
                    thirty_min_after_start VARCHAR(20),
                    at_end_time VARCHAR(20),
                    race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    FOREIGN KEY (race_id)
                        REFERENCES RACE (rowid) 
                    );"""
        cursor.execute(weather_sql)
        connection.commit()

        snow_condition_sql = """
                            CREATE TABLE IF NOT EXISTS SNOW_CONDITION(
                            thirty_min_before_start VARCHAR(20),
                            at_start_time VARCHAR(20),
                            thirty_min_after_start VARCHAR(20),
                            at_end_time VARCHAR(20),
                            race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                            FOREIGN KEY (race_id)
                                REFERENCES RACE (rowid) 
                            );"""
        cursor.execute(snow_condition_sql)
        connection.commit()

        snow_temperature_sql = """
                            CREATE TABLE IF NOT EXISTS SNOW_TEMPERATURE(
                            thirty_min_before_start REAL,
                            at_start_time REAL,
                            thirty_min_after_start REAL,
                            at_end_time REAL,
                            race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                            FOREIGN KEY (race_id)
                                REFERENCES RACE (rowid) 
                            );"""
        cursor.execute(snow_temperature_sql)
        connection.commit()

        air_temperature_sql = """
                            CREATE TABLE IF NOT EXISTS AIR_TEMPERATURE(
                            thirty_min_before_start REAL,
                            at_start_time REAL,
                            thirty_min_after_start REAL,
                            at_end_time REAL,
                            race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                            FOREIGN KEY (race_id)
                                REFERENCES RACE (rowid) 
                            );"""
        cursor.execute(air_temperature_sql)
        connection.commit()

        humidity_sql = """
                    CREATE TABLE IF NOT EXISTS HUMIDITY(
                    thirty_min_before_start INTEGER,
                    at_start_time INTEGER,
                    thirty_min_after_start INTEGER,
                    at_end_time INTEGER,
                    race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    FOREIGN KEY (race_id)
                        REFERENCES RACE (rowid) 
                    );"""
        cursor.execute(humidity_sql)
        connection.commit()

        wind_direction_sql = """
                            CREATE TABLE IF NOT EXISTS WIND_DIRECTION(
                            thirty_min_before_start VARCHAR(5),
                            at_start_time VARCHAR(5),
                            thirty_min_after_start VARCHAR(5),
                            at_end_time VARCHAR(5),
                            race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                            FOREIGN KEY (race_id)
                                REFERENCES RACE (rowid) 
                            );"""
        cursor.execute(wind_direction_sql)
        connection.commit()

        wind_speed_sql = """
                        CREATE TABLE IF NOT EXISTS WIND_SPEED(
                        thirty_min_before_start REAL,
                        at_start_time REAL,
                        thirty_min_after_start REAL,
                        at_end_time REAL,
                        race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                        FOREIGN KEY (race_id)
                            REFERENCES RACE (rowid) 
                        );"""
        cursor.execute(wind_speed_sql)
        connection.commit()

        course_sql = """
                    CREATE TABLE IF NOT EXISTS COURSE(
                    total_course_length INTEGER,
                    height_difference INTEGER,
                    max_climb INTEGER,
                    total_climb INTEGER,
                    level_difficulty VARCHAR(20),
                    race_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    FOREIGN KEY (race_id)
                        REFERENCES RACE (rowid) 
                    );"""
        cursor.execute(course_sql)
        connection.commit()

        race_data_sql = """
                        CREATE TABLE IF NOT EXISTS RACE_DATA(
                        name VARCHAR(40),
                        country VARCHAR(3),
                        total_misses INTEGER,
                        overall_time VARCHAR(20),
                        overall_time_behind VARCHAR(20) ,
                        overall_rank INTEGER,
                        cumulative_time_loop_one VARCHAR(20),
                        cumulative_time_loop_one_behind VARCHAR(20),
                        cumulative_time_loop_one_rank INTEGER,
                        cumulative_time_loop_two VARCHAR(20),
                        cumulative_time_loop_two_behind VARCHAR(20),
                        cumulative_time_loop_two_rank INTEGER,
                        cumulative_time_loop_three VARCHAR(20),
                        cumulative_time_loop_three_behind VARCHAR(20),
                        cumulative_time_loop_three_rank INTEGER,
                        cumulative_time_loop_four VARCHAR(20),
                        cumulative_time_loop_four_behind VARCHAR(20), 
                        cumulative_time_loop_four_rank INTEGER,
                        cumulative_time_overall VARCHAR(20),
                        cumulative_time_overall_behind VARCHAR(20),
                        cumulative_time_overall_rank INTEGER,
                        loop_time_loop_one VARCHAR(20),
                        loop_time_loop_one_behind VARCHAR(20),
                        loop_time_loop_one_rank INTEGER, 
                        loop_time_loop_two VARCHAR(20),
                        loop_time_loop_two_behind VARCHAR(20),
                        loop_time_loop_two_rank INTEGER,
                        loop_time_loop_three VARCHAR(20),
                        loop_time_loop_three_behind VARCHAR(20),
                        loop_time_loop_three_rank INTEGER,
                        loop_time_loop_four VARCHAR(20),
                        loop_time_loop_four_behind VARCHAR(20), 
                        loop_time_loop_four_rank INTEGER,
                        loop_time_loop_five VARCHAR(20),
                        loop_time_loop_five_behind VARCHAR(20), 
                        loop_time_loop_five_rank INTEGER,
                        shooting_misses_loop_one INTEGER,
                        shooting_time_loop_one VARCHAR(20),
                        shooting_time_loop_one_behind VARCHAR(20),
                        shooting_loop_one_rank INTEGER,
                        shooting_misses_loop_two INTEGER,
                        shooting_time_loop_two VARCHAR(20),
                        shooting_time_loop_two_behind VARCHAR(20),
                        shooting_loop_two_rank INTEGER,
                        shooting_misses_loop_three INTEGER,
                        shooting_time_loop_three VARCHAR(20),
                        shooting_time_loop_three_behind VARCHAR(20),
                        shooting_loop_three_rank INTEGER,
                        shooting_misses_loop_four INTEGER,
                        shooting_time_loop_four VARCHAR(20),
                        shooting_time_loop_four_behind VARCHAR(20),
                        shooting_loop_four_rank INTEGER,
                        shooting_misses_overall INTEGER,
                        shooting_time_overall VARCHAR(20),
                        shooting_time_overall_behind VARCHAR(20),
                        shooting_overall_rank INTEGER,
                        range_time_loop_one VARCHAR(20),
                        range_time_loop_one_behind VARCHAR(20),
                        range_time_loop_one_rank INTEGER,
                        range_time_loop_two VARCHAR(20),
                        range_time_loop_two_behind VARCHAR(20),
                        range_time_loop_two_rank INTEGER,
                        range_time_loop_three VARCHAR(20),
                        range_time_loop_three_behind VARCHAR(20),
                        range_time_loop_three_rank INTEGER,
                        range_time_loop_four VARCHAR(20),
                        range_time_loop_four_behind VARCHAR(20),
                        range_time_loop_four_rank INTEGER,
                        range_time_overall VARCHAR(20),
                        range_time_overall_behind VARCHAR(20),
                        range_time_overall_rank INTEGER,
                        course_time_loop_one VARCHAR(20),
                        course_time_loop_one_behind VARCHAR(20),
                        course_time_loop_one_rank INTEGER,
                        course_time_loop_two VARCHAR(20),
                        course_time_loop_two_behind VARCHAR(20),
                        course_time_loop_two_rank INTEGER,
                        course_time_loop_three VARCHAR(20),
                        course_time_loop_three_behind VARCHAR(20),
                        course_time_loop_three_rank INTEGER,
                        course_time_loop_four VARCHAR(20),
                        course_time_loop_four_behind VARCHAR(20),
                        course_time_loop_four_rank INTEGER,
                        course_time_loop_five VARCHAR(20),
                        course_time_loop_five_behind VARCHAR(20),
                        course_time_loop_five_rank INTEGER,
                        course_time_overall VARCHAR(20),
                        course_time_overall_behind VARCHAR(20),
                        course_time_overall_rank INTEGER,
                        penalty_time_loop_one VARCHAR(20), 
                        penalty_time_loop_two VARCHAR(20), 
                        penalty_time_loop_three VARCHAR(20), 
                        penalty_time_loop_four VARCHAR(20), 
                        penalty_time_overall VARCHAR(20), 
                        race_id INTEGER NOT NULL, 
                        athlete_id INTEGER NOT NULL,
                        FOREIGN KEY (race_id)
                            REFERENCES RACE (rowid)
                        FOREIGN KEY (athlete_id)
                            REFERENCES ATHLETE (rowid)
                        PRIMARY KEY(race_id, athlete_id)
                        );"""
        cursor.execute(race_data_sql)
        connection.commit()

        connection.close()
    except sqlite3.Error as serr:
        print("Please look into database_connection.create_json_and_db():", serr)
    finally:
        connection.close()

    # if there are already values in blacklist.json don't delete them by setting filled lists to emtpy lists
    try:
        with open('../Data/blacklist.json', 'r') as f:
            blacklist_dict = json.loads(f.read())
            if blacklist_dict["no_names"]:  # json file is not empty
                return
    except IOError as ioerr:
        print("Please look into database_connection.create_json_and_db()", ioerr)

    # create a json file with empty lists
    no_names = country = languages = hobbies = profession = skis = family = rifle = ammunition = \
        racesuit = shoes = bindings = skipoles = gloves = wax = goggles = []
    blacklist_dict = {"no_names": no_names, "country": country, "languages": languages,
                      "hobbies": hobbies, "profession": profession, "family": family,
                      "skis": skis, "rifle": rifle, "ammunition": ammunition, "racesuit": racesuit,
                      "shoes": shoes, "bindings": bindings, "skipoles": skipoles, "gloves": gloves,
                      "wax": wax, "goggles": goggles}
    try:
        with open('../Data/blacklist.json', 'w') as f:
            json.dump(blacklist_dict, f)
    except IOError as error:
        print("Please look into database_connection.create_json_and_db()", error)


def get_connection():
    """This method returns a connection to the database Data/Biathlon_Data."""
    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
    except sqlite3.Error as error:
        print("No connection to the database was possible."
              "Please look into database_connection.get_connection()", error)
    return connection


def get_json_lists():
    """This method returns different lists from the json file as a tuple.

        :returns:
            no_names: strings which are not names of an athlete
            country: list of already used countries (e.g. an other athlete)
            languages: list of already used countries (e.g. an other athlete)
            hobbies: list of already used hobbies (e.g. an other athlete)
            profession: list of already used professions (e.g. an other athlete)
            family: list of already used family types (e.g. an other athlete)
            skis: list of already used skis (e.g. an other athlete)
            rifle: list of already used rifles (e.g. an other athlete)
            ammunition: list of already used ammunition (e.g. an other athlete)
            racesuit: list of already used racesuits (e.g. an other athlete)
            shoes: list of already used shoes (e.g. an other athlete)
            bindings: list of already used bindings (e.g. an other athlete)
            skipoles: list of already used skipoles (e.g. an other athlete)
            gloves: list of already used gloves (e.g. an other athlete)
            wax: list of already used wax (e.g. an other athlete)
            goggles: list of already used goggles (e.g. an other athlete)

            All this lists except for no_names exists only as an orientation while creating a new
            athlete. It is a goal to name equal/similar things same.
            The list no_names exists for the program to remember strings which are no names. If the same
            string occurs in a different pdf_file the program knows already that this isn't a name and
            it skips that string without asking the user if it's a name or not.
            Therefore the user only once has to tell the program that a string is not a name.
    """
    try:
        with open('../Data/blacklist.json', 'r') as file:
            blacklist_dict = json.loads(file.read())
            no_names = blacklist_dict["no_names"]
            country = blacklist_dict["country"]
            languages = blacklist_dict["languages"]
            hobbies = blacklist_dict["hobbies"]
            profession = blacklist_dict["profession"]
            family = blacklist_dict["family"]
            skis = blacklist_dict["skis"]
            rifle = blacklist_dict["rifle"]
            ammunition = blacklist_dict["ammunition"]
            racesuit = blacklist_dict["racesuit"]
            shoes = blacklist_dict["shoes"]
            bindings = blacklist_dict["bindings"]
            skipoles = blacklist_dict["skipoles"]
            gloves = blacklist_dict["gloves"]
            wax = blacklist_dict["wax"]
            goggles = blacklist_dict["goggles"]
        return no_names, country, languages, hobbies, profession, family, skis, rifle, ammunition, \
               racesuit, shoes, bindings, skipoles, gloves, wax, goggles
    except IOError as error:
        print("Please look into database_connection.get_json_lists():", error)


def set_json_lists(json_list):
    """This method updates the lists in the json file.

        :arg:
            json_list (str): list of updated lists

    """
    if not isinstance(json_list, list):
        raise TypeError
    if len(json_list) != 16:
        raise IndexError
    for ls in json_list:
        if not isinstance(ls, list):
            raise TypeError

    blacklist_dict = {"no_names": json_list[0], "country": json_list[1], "languages": json_list[2],
                      "hobbies": json_list[3], "profession": json_list[4], "family": json_list[5],
                      "skis": json_list[6], "rifle": json_list[7], "ammunition": json_list[8],
                      "racesuit": json_list[9], "shoes": json_list[10], "bindings": json_list[11],
                      "skipoles": json_list[12], "gloves": json_list[13], "wax": json_list[14],
                      "goggles": json_list[15]}

    try:
        with open('../Data/blacklist.json', 'w') as f:
            json.dump(blacklist_dict, f)
    except IOError as error:
        print("Please look into database_connection set_json_lists", error)


def update_tables(race_id):
    """This method updates the other tables through creating new rows with the race_id as primary key.

        :arg:
            race_id (Integer): the race_id determines the primary key of the newly created rows which belong
            to the race with this race_id in the table RACE
    """
    conn = get_connection()
    cursor = conn.cursor()

    # The following lines check the different tables for the race_id.
    # If a row with the race_id is already existing nothing will be done.
    # If no such row is existing one is created.
    # This also supports the 'UNIQUE' attribute of the race_id.
    get_course = "SELECT * FROM COURSE WHERE race_id = ?"
    cursor.execute(get_course, (race_id,))
    course = cursor.fetchall()
    if not course:  # there exists no row with the race_id -> create a row with the race_id
        update_course = "INSERT INTO COURSE (race_id) VALUES (?);"
        cursor.execute(update_course, (race_id,))
        conn.commit()

    get_snow_cond = "SELECT * FROM SNOW_CONDITION WHERE race_id = ?"
    cursor.execute(get_snow_cond, (race_id,))
    snow_cond = cursor.fetchall()
    if not snow_cond:  # there exists no row with the race_id -> create this row
        update_snow_cond = "INSERT INTO SNOW_CONDITION (race_id) VALUES (?);"
        cursor.execute(update_snow_cond, (race_id,))
        conn.commit()

    get_snow_temp = "SELECT * FROM SNOW_TEMPERATURE WHERE race_id = ?"
    cursor.execute(get_snow_temp, (race_id,))
    snow_temp = cursor.fetchall()
    if not snow_temp:  # there exists no row with the race_id -> create this row
        update_snow_temp = "INSERT INTO SNOW_TEMPERATURE (race_id) VALUES (?);"
        cursor.execute(update_snow_temp, (race_id,))
        conn.commit()

    get_air_temp = "SELECT * FROM AIR_TEMPERATURE WHERE race_id = ?"
    cursor.execute(get_air_temp, (race_id,))
    air_temp = cursor.fetchall()
    if not air_temp:  # there exists no row with the race_id -> create this row
        update_air_temp = "INSERT INTO AIR_TEMPERATURE (race_id) VALUES (?);"
        cursor.execute(update_air_temp, (race_id,))
        conn.commit()

    get_hum = "SELECT * FROM HUMIDITY WHERE race_id = ?"
    cursor.execute(get_hum, (race_id,))
    hum = cursor.fetchall()
    if not hum:  # there exists no row with the race_id -> create this row
        update_hum = "INSERT INTO HUMIDITY (race_id) VALUES (?);"
        cursor.execute(update_hum, (race_id,))
        conn.commit()

    get_weather = "SELECT * FROM WEATHER WHERE race_id = ?"
    cursor.execute(get_weather, (race_id,))
    weather = cursor.fetchall()
    if not weather:  # there exists no row with the race_id -> create this row
        update_weather = "INSERT INTO WEATHER (race_id) VALUES (?);"
        cursor.execute(update_weather, (race_id,))
        conn.commit()

    get_wind_dir = "SELECT * FROM WIND_DIRECTION WHERE race_id = ?"
    cursor.execute(get_wind_dir, (race_id,))
    wind_dir = cursor.fetchall()
    if not wind_dir:  # there exists no row with the race_id -> create this row
        update_wind_dir = "INSERT INTO WIND_DIRECTION (race_id) VALUES (?);"
        cursor.execute(update_wind_dir, (race_id,))
        conn.commit()

    get_wind_speed = "SELECT * FROM WIND_SPEED WHERE race_id = ?"
    cursor.execute(get_wind_speed, (race_id,))
    wind_speed = cursor.fetchall()
    if not wind_speed:  # there exists no row with the race_id -> create this row
        update_wind_speed = "INSERT INTO WIND_SPEED (race_id) VALUES (?);"
        cursor.execute(update_wind_speed, (race_id,))
        conn.commit()

    conn.close()


def create_race(biathlon_obj):
    """This method tests if the race associated to the biathlon_obj already exists in the database table RACE.

        If it does not exist, a new row is inserted with the ID of the race.
        :arg:
            biathlon_obj (BiathlonData object): object associated with a pdf file, contains metadata dictionary
        :return
            the race ID from the database associated with this race/biathlon_obj
    """
    # tests if race already exists
    place = biathlon_obj.metadata['place']
    year = biathlon_obj.metadata['date'].year
    race_type = biathlon_obj.metadata['race_type']
    age_group = biathlon_obj.metadata['age_group']
    gender = biathlon_obj.metadata['gender']
    race_characteristics = (place, year, race_type, age_group, gender)
    if None in race_characteristics:
        raise Exception('This should not happen. Please look into database_connection.create_race()')

    conn = get_connection()
    cursor = conn.cursor()
    get_rowid_query = "SELECT ROWID FROM RACE WHERE place = ? AND year = ? AND type = ? AND age = ? AND" \
                      " gender = ?;"
    cursor.execute(get_rowid_query, race_characteristics)
    row_id = cursor.fetchall()
    if len(row_id) > 1:
        raise Exception('More than one row exists for this race. Race cannot be identified certainly')

    if row_id:
        race_id = row_id[0][0]  # row_id has following format: ((int,))
    else:  # no row with the race characteristics exists -> one is created
        new_row_query = "INSERT INTO RACE (place, year, type, age, gender) " \
                        "VALUES(?, ?, ?, ?, ?);"
        cursor.execute(new_row_query, race_characteristics)
        conn.commit()
        cursor.execute(get_rowid_query, race_characteristics)
        row_id = cursor.fetchall()
        race_id = row_id[0][0]

    # update the other tables by creating new rows with the race_id as primary key
    update_tables(race_id)
    return race_id


def metadata_to_database(biathlon_obj):
    """This method updates the database table Race with metadata from a pdf file.

        1. metadata value is None:
            nothing is updated and done
        2. metadata value is not None AND database value is not NULL:
            raises an exception if the metadata value differs from the value in the database
        3. metadata value is not None AND database value is NULL:
            The table Race is updated when the database has a NULL value (e.g. race_len_km = NULL) and
            the metadata of a BiathlonData object associated with a pdf file has a value different from
            None (e.g. biathlon_obj.metadata['race_len_km'] = '12.5KM'). The updated value is the value of
            the metadata of a BiathlonData object.

        :arg:
            biathlon_obj(BiathlonData): object associated with a pdf file, contains metadata dictionary

    """
    if not isinstance(biathlon_obj, ed.BiathlonData):
        raise TypeError
    conn = get_connection()
    cursor = conn.cursor()
    race_id = create_race(biathlon_obj)
    if not isinstance(race_id, int):
        raise ValueError

    # get the values of the row associated to the race_id
    get_course_row_query = "SELECT total_course_length, height_difference, max_climb," \
                           " total_climb, level_difficulty FROM COURSE WHERE race_id = ?;"
    cursor.execute(get_course_row_query, (race_id,))
    course_row = cursor.fetchall()

    get_race_row_query = "SELECT race_len_km, number_of_entries, did_not_start, did_not_finish, lapped, disqualified, " \
                         "disqualified_for_unsportsmanlike_behaviour, ranked FROM RACE WHERE ROWID = ?; "
    cursor.execute(get_race_row_query, (race_id,))
    race_row = cursor.fetchall()
    get_weather_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                            "at_end_time FROM WEATHER WHERE race_id = ?;"
    cursor.execute(get_weather_row_query, (race_id,))
    weather_row = cursor.fetchall()

    get_snow_cond_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                              "at_end_time FROM SNOW_CONDITION WHERE race_id = ?;"
    cursor.execute(get_snow_cond_row_query, (race_id,))
    snow_cond_row = cursor.fetchall()

    get_snow_temp_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                              "at_end_time FROM SNOW_TEMPERATURE WHERE race_id = ?;"
    cursor.execute(get_snow_temp_row_query, (race_id,))
    snow_temp_row = cursor.fetchall()

    get_air_temp_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                             "at_end_time FROM AIR_TEMPERATURE WHERE race_id = ?;"
    cursor.execute(get_air_temp_row_query, (race_id,))
    air_temp_row = cursor.fetchall()

    get_humid_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                          "at_end_time FROM HUMIDITY WHERE race_id = ?;"
    cursor.execute(get_humid_row_query, (race_id,))
    humid_row = cursor.fetchall()

    get_wind_dir_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                             "at_end_time FROM WIND_DIRECTION WHERE race_id = ?;"
    cursor.execute(get_wind_dir_row_query, (race_id,))
    wind_dir_row = cursor.fetchall()

    get_wind_speed_row_query = "SELECT thirty_min_before_start, at_start_time, thirty_min_after_start," \
                               "at_end_time FROM WIND_SPEED WHERE race_id = ?;"
    cursor.execute(get_wind_speed_row_query, (race_id,))
    wind_speed_row = cursor.fetchall()

    row = [elem for elem in race_row[0]]
    row.append(list(weather_row[0]))
    row.append(list(snow_cond_row[0]))
    row.append(list(snow_temp_row[0]))
    row.append(list(air_temp_row[0]))
    row.append(list(humid_row[0]))
    row.append(list(wind_dir_row[0]))
    row.append(list(wind_speed_row[0]))
    for t in course_row[0]:
        row.append(t)
    # print(row)

    metadata_tuple = [biathlon_obj.metadata['race_len_km'], biathlon_obj.metadata['number_of_entries'],
                      biathlon_obj.metadata['did_not_start'],
                      biathlon_obj.metadata['did_not_finish'], biathlon_obj.metadata['lapped'],
                      biathlon_obj.metadata['disqualified'],
                      biathlon_obj.metadata['disqualified_for_unsportsmanlike_behaviour'],
                      biathlon_obj.metadata['ranked'], biathlon_obj.metadata['weather'],
                      biathlon_obj.metadata['snow_condition'],
                      biathlon_obj.metadata['snow_temperature'], biathlon_obj.metadata['air_temperature'],
                      biathlon_obj.metadata['humidity'], biathlon_obj.metadata['wind_direction'],
                      biathlon_obj.metadata['wind_speed'], biathlon_obj.metadata['total_course_length'],
                      biathlon_obj.metadata['height_difference'], biathlon_obj.metadata['max_climb'],
                      biathlon_obj.metadata['total_climb'], biathlon_obj.metadata['level_difficulty']]

    # row and metadata_tuple have the same length
    for index, i in enumerate(zip(metadata_tuple, row)):
        # print(index, i)
        if i[0] is None:  # no usable data in biathlon_object.metadata
            continue
        if i[1] is None or (isinstance(i[1], list) and all(v is None for v in i[1])):
            # database has a NULL/None value and biathlon_object.metadata has a usable value
            # There is probably a faster way to code this. !!!!!!!!!!!!!!!!!
            if index == 0:
                sql_query_3 = "UPDATE RACE SET race_len_km = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 1:
                sql_query_3 = "UPDATE RACE SET number_of_entries = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 2:
                sql_query_3 = "UPDATE RACE SET did_not_start = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 3:
                sql_query_3 = "UPDATE RACE SET did_not_finish = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 4:
                sql_query_3 = "UPDATE RACE SET lapped = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 5:
                sql_query_3 = "UPDATE RACE SET disqualified = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 6:
                sql_query_3 = "UPDATE RACE SET disqualified_for_unsportsmanlike_behaviour = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 7:
                sql_query_3 = "UPDATE RACE SET ranked = ?  WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 8:
                sql_query_3 = "UPDATE WEATHER SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 9:
                sql_query_3 = "UPDATE SNOW_CONDITION SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 10:
                sql_query_3 = "UPDATE SNOW_TEMPERATURE SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 11:
                sql_query_3 = "UPDATE AIR_TEMPERATURE SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 12:
                sql_query_3 = "UPDATE HUMIDITY SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 13:
                sql_query_3 = "UPDATE WIND_DIRECTION SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 14:
                sql_query_3 = "UPDATE WIND_SPEED SET thirty_min_before_start = ?, at_start_time = ?, " \
                              "thirty_min_after_start = ?, at_end_time = ? WHERE rowid = ?; "
                cursor.execute(sql_query_3, (i[0][0], i[0][1], i[0][2], i[0][3], race_id))
                conn.commit()
            elif index == 15:
                sql_query_3 = "UPDATE COURSE SET total_course_length = ? WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 16:
                sql_query_3 = "UPDATE COURSE SET height_difference = ? WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 17:
                sql_query_3 = "UPDATE COURSE SET max_climb = ? WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 18:
                sql_query_3 = "UPDATE COURSE SET total_climb = ? WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
            elif index == 19:
                sql_query_3 = "UPDATE COURSE SET level_difficulty = ? WHERE rowid = ?;"
                cursor.execute(sql_query_3, (i[0], race_id))
                conn.commit()
        else:
            if i[0] != i[1]:
                raise Exception('Difference between database and a BiathlonData object'
                                'This should not happen. Please look into data_to_database')


def race_data_to_database(biathlon_obj):
    """This method transforms the data from a pd.Dataframe to the database table RACE_DATA"""
    if not isinstance(biathlon_obj, ed.BiathlonData):
        return
    if biathlon_obj.data is None or not isinstance(biathlon_obj.data, pd.DataFrame):
        return

    # if it doesn't already exist create_race() creates a new tuple in the database table RACE
    # and returns then the race_id associated to that tuple
    # if it does exist it returns the race_id associated to the fitting tuple
    race_id = create_race(biathlon_obj)

    conn = get_connection()
    cursor = conn.cursor()

    # looping through the race_data which is stored in a pd.Dataframe
    for index, row in biathlon_obj.data.iterrows():

        # get athlete_id of the athlete which is connected to a row in the pd.Dataframe
        get_athlete_id_query = "SELECT ROWID FROM ATHLETE WHERE name = ?;"
        cursor.execute(get_athlete_id_query, (row[0],))
        athlete_id = cursor.fetchall()
        if not athlete_id:
            raise Exception("This should not happen. Please make sure every athlete that finished in this"
                            "race already has his tuple in the table ATHLETE.")
        # checks if a tuple with the primary keys athlete_id and race_id already exists  in RACE_DATA
        # and creates one if not
        get_race_data_rows = "SELECT * FROM RACE_DATA WHERE athlete_id = ? AND race_id = ?"
        cursor.execute(get_race_data_rows, (athlete_id, race_id))
        rows_race_data = cursor.fetchall()
        if not rows_race_data:  # create rows if they don't exist yet
            create_race_instance_query = "INSERT INTO RACE_DATA (athlete_id, race_id)" \
                                         "VALUES (?,?);"
            cursor.execute(create_race_instance_query, (athlete_id, race_id))
            conn.commit()

        # only if the value in the pd.Dataframe row isn't NaN we insert it into the database
        row_filtered = row.isna()
        # There is probably a faster way to code this
        for col, b in enumerate(zip(row_filtered, row)):
            if b[0]:  # value in this row and column is NaN
                continue
            if col == 0:
                insert_race_data_query = "UPDATE RACE_DATA SET name = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 1:
                insert_race_data_query = "UPDATE RACE_DATA SET country = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 2:
                insert_race_data_query = "UPDATE RACE_DATA SET total_misses = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 3:
                insert_race_data_query = "UPDATE RACE_DATA SET overall_time = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 4:
                insert_race_data_query = "UPDATE RACE_DATA SET overall_time_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 5:
                insert_race_data_query = "UPDATE RACE_DATA SET overall_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 6:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 7:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_one_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 8:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_one_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 9:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 10:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_two_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 11:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_two_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 12:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 13:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_three_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 14:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_three_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 15:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 16:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_four_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 17:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_loop_four_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 18:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 19:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_overall_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 20:
                insert_race_data_query = "UPDATE RACE_DATA SET cumulative_time_overall_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 21:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 22:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_one_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 23:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_one_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 24:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 25:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_two_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 26:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_two_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 27:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 28:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_three_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 29:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_three_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 30:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 31:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_four_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 32:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_four_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 33:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_five = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 34:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_five_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 35:
                insert_race_data_query = "UPDATE RACE_DATA SET loop_time_loop_five_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 36:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_misses_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 37:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 38:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_one_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 39:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_loop_one_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 40:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_misses_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 41:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 42:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_two_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 43:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_loop_two_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 44:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_misses_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 45:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 46:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_three_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 47:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_loop_three_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 48:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_misses_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 49:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 50:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_loop_four_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 51:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_loop_four_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 52:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_misses_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 53:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 54:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_time_overall_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 55:
                insert_race_data_query = "UPDATE RACE_DATA SET shooting_overall_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 56:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 57:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_one_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 58:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_one_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 59:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 60:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_two_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 61:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_two_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 62:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 63:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_three_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 64:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_three_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 65:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 66:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_four_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 67:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_loop_four_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 68:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 69:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_overall_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 70:
                insert_race_data_query = "UPDATE RACE_DATA SET range_time_overall_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 71:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 72:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_one_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 73:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_one_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 74:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 75:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_two_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 76:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_two_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 77:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 78:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_three_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 79:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_three_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 80:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 81:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_four_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 82:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_four_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 83:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_five = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 84:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_five_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 85:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_loop_five_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 86:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 87:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_overall_behind = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 88:
                insert_race_data_query = "UPDATE RACE_DATA SET course_time_overall_rank = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 89:
                insert_race_data_query = "UPDATE RACE_DATA SET penalty_time_loop_one = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 90:
                insert_race_data_query = "UPDATE RACE_DATA SET penalty_time_loop_two = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 91:
                insert_race_data_query = "UPDATE RACE_DATA SET penalty_time_loop_three = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 92:
                insert_race_data_query = "UPDATE RACE_DATA SET penalty_time_loop_four = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
            elif col == 93:
                insert_race_data_query = "UPDATE RACE_DATA SET penalty_time_overall = ? " \
                                         "WHERE athlete_id = ? AND race_id = ?;"
                cursor.execute(insert_race_data_query, (b[1], athlete_id, race_id))
                conn.commit()
    conn.close()
