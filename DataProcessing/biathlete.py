import json
import sqlite3


def create_json_and_db():
    """This method creates a json file and a database table Athlete.

        This method should only be invoked once at the beginning. If this method is invoked at a later
        date then all json lists reset to empty lists
    """
    # create an Athlete table if it doesn't already exist
    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
        cursor = connection.cursor()
        creation_sql = """
            CREATE TABLE IF NOT EXISTS Athlete (
            name VARCHAR(40) NOT NULL, 
            birthdate VARCHAR(50) NOT NULL, 
            country VARCHAR(3) NOT NULL,
            languages VARCHAR(400) ,
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
            PRIMARY KEY(name, birthdate, country)
            );"""
        cursor.execute(creation_sql)
        connection.commit()
        connection.close()
    except sqlite3.Error:
        print("Please look into DataProcessing.biathlete create_json_and_db at database creation")
    finally:
        connection.close()

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
        print("Please look into DataProcessing.biathlete create_json_and_db at json creation")


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
        print("Please look into DataProcessing.biathlete get_json_lists()")


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
        print("Please look into DataProcessing.biathlete set_json_lists")


def get_connection():
    """This method returns a connection to the database Data/Biathlon_Data"""

    try:
        connection = sqlite3.connect("../Data/Biathlon_Data.db")
    except sqlite3.Error as error:
        print("Failed to insert data from table Athlete", error)
    return connection


def get_all_athletes():
    """This method gets the name of the athletes from the table 'Athlete' of the database"""

    connection = get_connection()
    try:
        cursor = connection.cursor()
        sql_names_country = "SELECT name FROM Athlete"
        cursor.execute(sql_names_country)
        rows = cursor.fetchall()
        name_column = []
        for row in rows:
            name_column.append(row[0])
        return name_column
    except sqlite3.Error as error:
        print("Failed to read data from table Athlete", error)
        return None


def create_athlete(athlete_name):
    """This method creates a new athlete tuple/row in the database (table = Athlete)

        Args:
            athlete_name (str): name of the athlete
    """
    if not isinstance(athlete_name, str):
        raise TypeError

    print("This is the athlete's name we want to create: ", athlete_name)
    connection = get_connection()

    # json values
    try:
        no_names, country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit, \
        shoes, bindings, skipoles, gloves, wax, goggles = get_json_lists()
    except TypeError:
        print("This should work to create an athlete. Please look into get_json_lists()")
        return

    # input of an athlete's data
    print("Please insert STOP if you misspelled an input before")
    print("Please try to use same names for same/similar things. If data is not existing"
          " use NULL")
    birthdate_inp = input('Please enter the birthdate (in YYYY-MM-DD as string): ')

    print("list of countries: ", country)
    country_inp = input('Please enter the nationality of the athlete: ')

    print("list of languages: ", languages)
    languages_inp = []
    while True:
        another_language = input("Another language? (=y for YES). If NULL, press 'y'.: ")
        if another_language == "y":
            languages_inp.append(input('Please enter another language of the athlete: '))
        else:
            break

    print("list of hobbies: ", hobbies)
    hobbies_inp = []
    while True:
        another_hobby = input("Another hobby? (=y for YES). If NULL, press 'y'.: ")
        if another_hobby == "y":
            hobbies_inp.append(input('Please enter another hobby of the athlete: '))
        else:
            break

    print("list of professions: ", profession)
    profession_inp = input('Please enter the profession of the athlete: ')

    print("list of family types: ", family)
    family_inp = input('Please enter the family status of the athlete: ')

    print("list of skis: ", skis)
    skis_inp = input('Please enter the company which provides skis for the athlete: ')

    print("list of rifles: ", rifle)
    rifle_inp = input('Please enter the company which provides the rifle for the athlete: ')

    print("list of ammunition: ", ammunition)
    ammunition_inp = input('Please enter the company which provides ammunition for the athlete: ')

    print("list of racesuits: ", racesuit)
    racesuit_inp = input('Please enter the company which provides the racesuit for the athlete: ')

    print("list of shoes: ", shoes)
    shoes_inp = input('Please enter the company which provides shoes for the athlete: ')

    print("list of bindings: ", bindings)
    bindings_inp = input('Please enter the company which provides bindings for the athlete: ')

    print("list of skipoles: ", skipoles)
    skipoles_inp = input('Please enter the company which provides skipoles for the athlete: ')

    print("list of gloves: ", gloves)
    gloves_inp = input('Please enter the company which provides gloves for the athlete: ')

    print("list of wax: ", wax)
    wax_inp = input('Please enter the company which provides wax for the athlete: ')

    print("list of goggles: ", goggles)
    goggles_inp = input('Please enter the company which provides goggles for the athlete: ')

    while True:
        size_inp = input('Please enter the size of the athlete: ')
        weight_inp = input('Please enter the weight of the athlete: ')
        try:
            size_inp = int(size_inp)
            weight_inp = int(weight_inp)
            break
        except ValueError:
            print("Size and weight have to be integers! Please try again.")
            create_athlete(athlete_name)  # try again
            return

    current_ls = [country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit,
                  shoes, bindings, skipoles, gloves, wax, goggles]
    update_ls = [country_inp, languages_inp, hobbies_inp, profession_inp, family_inp, skis_inp,
                 rifle_inp, ammunition_inp, racesuit_inp, shoes_inp, bindings_inp, skipoles_inp,
                 gloves_inp, wax_inp, goggles_inp]

    for stop in update_ls:
        if type(stop) == list:
            if "STOP" in stop:
                print("Please try again")
                create_athlete(athlete_name)
                return
        elif stop == "STOP":
            create_athlete(athlete_name)
            return

    # replace 'NULL' values with None
    for cur, upd in zip(current_ls, update_ls):
        if type(upd) == list and len(upd) > 0:  # upd is a list
            for i in upd:
                if i not in cur:
                    cur.append(i)
        else:  # upd is a string
            if upd not in cur:
                cur.append(upd)

    current_ls.insert(0, no_names)

    # update json file
    set_json_lists(current_ls)

    # insert into database
    insert_tuple = (athlete_name, birthdate_inp, country_inp, str(languages_inp), str(hobbies_inp),
                    profession_inp, family_inp, skis_inp, rifle_inp, ammunition_inp, racesuit_inp,
                    shoes_inp, bindings_inp, skipoles_inp, gloves_inp, wax_inp, goggles_inp, size_inp,
                    weight_inp)

    sql_insert = """INSERT INTO Athlete(name, birthdate, country, languages, hobbies, profession, family,
                     skis, rifle, ammunition, racesuit, shoes, bindings, skipoles, gloves, wax, goggles,
                     size, weight)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor = connection.cursor()
    cursor.execute(sql_insert, insert_tuple)
    connection.commit()
    print("Successfully added a new athlete.")


def update_athlete_db(text_ls):
    """This method decides for every string in a document if it is a name of an athlete.

        Args:
            text_ls (str list): text of a pdf document

        Returns:
            index_list (integer list): indices in text_ls which point to a name of a biathlete
    """
    if not isinstance(text_ls, list):
        raise TypeError
    for j in text_ls:
        if not isinstance(j, str):
            raise TypeError
    index_list = []
    connection = get_connection()
    for index, text in enumerate(text_ls):

        # json values
        no_names, country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit, \
        shoes, bindings, skipoles, gloves, wax, goggles = get_json_lists()

        # if text is in the list of no_names text can be skipped
        if text in no_names:
            continue

        # !!! Athlete with same names will have the same "ID". We will separate two different athletes
        # with the same name later through tests.
        # This could cause a mistake in the database !!!!!!!!!!!!!
        athlete_names = get_all_athletes()
        if text in athlete_names:
            index_list.append(index)
            continue

        # go to next element in text_ls because a name of an athlete has no integers
        for j in text:
            if j.isdigit():
                break
        else:
            print(text)
            inp = input("Please decide if this is a name(=y for YES) or not (else): ")
            if inp == "y":
                index_list.append(index)
                create_athlete(text)
            else:  # adds text to the list of no_names
                no_names.append(text)
                current_ls = [no_names, country, languages, hobbies, profession, family, skis, rifle,
                              ammunition, racesuit, shoes, bindings, skipoles, gloves, wax, goggles]
                set_json_lists(current_ls)
    connection.close()
    return index_list
