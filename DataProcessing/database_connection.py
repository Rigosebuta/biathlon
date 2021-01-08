import json
import sqlite3


def create_json_and_db():
    """This method creates a json file and a database.

        This method should only be invoked once at the beginning. If this method is invoked at a later
        date then all json lists reset to empty lists
    """
    # create an Athlete table if it doesn't already exist
    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
        cursor = connection.cursor()
        athlete_sql = """
            CREATE TABLE IF NOT EXISTS Athlete (
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
                    CREATE TABLE IF NOT EXISTS Race (
                    place VARCHAR(20) NOT NULL,
                    date VARCHAR(20) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    age VARCHAR(20) NOT NULL,
                    gender VARCHAR(20) NOT NULL,
                    race_length_km VARCHAR(20),
                    number_of_entries Integer,
                    did_not_start Integer,
                    did_not_finish Integer,
                    lapped Integer,
                    disqualified Integer,
                    disqualified_for_unsportsmanlike_behaviour Integer,
                    ranked Integer,
                    weather VARCHAR(60),
                    snow_condition VARCHAR(60),
                    snow_temperature VARCHAR(60),
                    air_temperature VARCHAR(60),
                    humidity VARCHAR(60),
                    wind_direction VARCHAR(60),
                    wind_speed VARCHAR(60),
                    total_course_length Integer,
                    height_difference Integer,
                    max_climb Integer,
                    total_climb Integer,
                    level_difficulty VARCHAR(20),
                    PRIMARY KEY(place, date, type, age, gender)
                    );"""
        cursor.execute(race_sql)
        connection.commit()

        connection.close()
    except sqlite3.Error:
        print("Please look into database_connection", 77)
    finally:
        connection.close()

    # if there are already values in blacklist.json don't delete them setting filled lists to emtpy lists
    try:
        with open('../Data/blacklist.json', 'r') as f:
            blacklist_dict = json.loads(f.read())
            if blacklist_dict["no_names"]:
                return
    except IOError:
        print("Please look into database_connection")

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
    except IOError:
        print("Please look into database_connection")


def get_connection():
    """This method returns a connection to the database Data/Biathlon_Data"""

    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
    except sqlite3.Error as error:
        print("Failed to insert data from table Athlete", error)
    return connection


def get_json_lists():
    """This method returns different lists from the json file as a tuple

        Returns:
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
    except IOError:
        print("Please look into database_connection.")


def set_json_lists(json_list):
    """This method updates the lists in the json file

        Args:
            json_list (str): list of updated json lists

    """

    if len(json_list) < 16:
        raise IndexError
    for i in json_list:
        if not isinstance(i, list):
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
    except IOError:
        print("Please look into database_connection set_json_lists")
