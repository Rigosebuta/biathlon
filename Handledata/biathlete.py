import sqlite3
import json


def create_json_and_db():
    connection = sqlite3.connect("Data/Biathlon_Data.db")
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
        PRIMARY KEY(name, birthdate, country)
        );"""
    cursor.execute(creation_sql)
    connection.commit()
    connection.close()

    # create a json file with empty lists
    no_names = country = languages = hobbies = profession = skis = family = rifle = ammunition = \
        racesuit = shoes = bindings = skipoles = gloves = wax = goggles = []
    blacklist_dict = {"no_names": no_names, "country": country, "languages": languages,
                      "hobbies": hobbies, "profession": profession, "family": family,
                      "skis": skis, "rifle": rifle, "ammunition": ammunition, "racesuit": racesuit,
                      "shoes": shoes, "bindings": bindings, "skipoles": skipoles, "gloves": gloves,
                      "wax": wax, "goggles": goggles}
    json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))


def get_all_athletes():
    """This method gets the name of the athletes from the table 'Athlete' of the database"""
    try:
        connection = sqlite3.connect("Data/Biathlon_Data.db")
        cursor = connection.cursor()
        sql_names_country = "SELECT name, country FROM Athlete"
        cursor.execute(sql_names_country)
        rows = cursor.fetchall()
        name_column = []
        for row in rows:
            name_column.append(row[0])
        connection.close()
        return name_column
    except sqlite3.Error as error:
        print("Failed to read data from table Athlete", error)
    finally:
        connection.close()
        return name_column


def create_athlete(athlete_name):
    print("This is the athlete's name we want to create: ", athlete_name)
    with open('Data/blacklist.json', 'r') as file:
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
    print("Please try to use same names for same/similar things. If data is not existing"
          "use NOT")
    birthdate_inp = input('Please enter the birthdate (in YYYY-MM-DD as string): ')
    print("list of countries", country)
    country_inp = input('Please enter the nationality of the athlete: ')
    print("list of languages", languages)
    languages_inp = []
    while True:
        another_language = input("Another language?(=y for YES)")
        if another_language == "y":
            languages_inp.append(input('Please enter another language of the athlete: '))
        else:
            break

    print("list of hobbies", hobbies)
    hobbies_inp = []
    while True:
        another_hobby = input("Another hobby?(=y for YES)")
        if another_hobby == "y":
            hobbies_inp.append(input('Please enter another hobby of the athlete: '))
        else:
            break

    print("list of professions", profession)
    profession_inp = input('Please enter the profession of the athlete: ')
    print("list of family types", family)
    family_inp = input('Please enter the family status of the athlete: ')
    print("list of skis", skis)
    skis_inp = input('Please enter the company which provides skis for the athlete: ')
    print("list of rifles", rifle)
    rifle_inp = input('Please enter the company which provides the rifle for the athlete: ')
    print("list of ammunition", ammunition)
    ammunition_inp = input('Please enter the company which provides ammunition for the athlete: ')
    print("list of racesuits", racesuit)
    racesuit_inp = input('Please enter the company which provides the racesuit for the athlete: ')
    print("list of shoes", shoes)
    shoes_inp = input('Please enter the company which provides shoes for the athlete: ')
    print("list of bindings", bindings)
    bindings_inp = input('Please enter the company which provides bindings for the athlete: ')
    print("list of skipoles", skipoles)
    skipoles_inp = input('Please enter the company which provides skipoles for the athlete: ')
    print("list of gloves", gloves)
    gloves_inp = input('Please enter the company which provides gloves for the athlete: ')
    print("list of wax", wax)
    wax_inp = input('Please enter the company which provides wax for the athlete: ')
    print("list of goggles", goggles)
    goggles_inp = input('Please enter the company which provides goggles for the athlete: ')

    current_ls = [country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit, shoes,
                  bindings, skipoles, gloves, wax, goggles]
    update_ls = [country_inp, languages_inp, hobbies_inp, profession_inp, family_inp, skis_inp, rifle_inp,
                 ammunition_inp, racesuit_inp, shoes_inp, bindings_inp, skipoles_inp, gloves_inp,
                 wax_inp, goggles_inp]
    for cur, upd in zip(current_ls, update_ls):
        if type(upd) == list:
            for i in upd:
                if i not in cur:
                    cur.append(i)
        else:  # upd is a string
            if upd not in cur:
                cur.append(upd)

    blacklist_dict = {"no_names": no_names, "country": country, "languages": languages,
                      "hobbies": hobbies, "profession": profession, "family": family,
                      "skis": skis, "rifle": rifle, "ammunition": ammunition,
                      "racesuit": racesuit, "shoes": shoes, "bindings": bindings,
                      "skipoles": skipoles, "gloves": gloves, "wax": wax, "goggles": goggles}
    json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))

    insert_tuple = (athlete_name, birthdate_inp, country_inp, str(languages_inp), str(hobbies_inp),
                     profession_inp, family_inp, skis_inp, rifle_inp, ammunition_inp, racesuit_inp,
                      shoes_inp, bindings_inp, skipoles_inp, gloves_inp, wax_inp, goggles_inp)
    for j in insert_tuple:
        if not type(j) == str:
            print(j)
    sql_insert = """INSERT INTO Athlete
                    (name, birthdate, country, languages, hobbies, profession, family,
                     skis, rifle, ammunition, racesuit, shoes, bindings,
                      skipoles, gloves, wax, goggles)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? , ?);"""
    try:
        connection = sqlite3.connect("Data/Biathlon_Data.db")
        cursor = connection.cursor()
        cursor.execute(sql_insert, insert_tuple)
        connection.close()
    except sqlite3.Error as error:
        print("Failed to insert data from table Athlete", error)

    finally:
        connection.close()


def update_athlete_db(text_ls):
    """"""
    for text in text_ls:

        with open('Data/blacklist.json', 'r') as file:
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

        if text in no_names:
            continue

        athlete_names = get_all_athletes()
        #rint(athlete_names)
        if text in athlete_names:
            continue

        for j in text:  # go to next element in text_ls because a name of an athlete has no integers
            if j.isdigit():
                break
        else:
            print(text)
            inp = input("Please decide if this is a name(=y for YES) or not (=n for NO): ")
            if inp == "y":
                create_athlete(text)
            elif inp == 'n':
                no_names.append(text)
                blacklist_dict = {"no_names": no_names, "country": country, "languages": languages,
                                  "hobbies": hobbies, "profession": profession, "family": family,
                                  "skis": skis, "rifle": rifle, "ammunition": ammunition,
                                  "racesuit": racesuit, "shoes": shoes, "bindings": bindings,
                                  "skipoles": skipoles, "gloves": gloves, "wax": wax, "goggles": goggles}
                json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))
            else:
                continue
