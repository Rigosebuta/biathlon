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
    blacklist_dict = {"no_names": no_names, "country": country, "languages": languages, "hobbies": hobbies,
                      "profession": profession, "family": family, "skis": skis, "rifle": rifle,
                      "ammunition": ammunition, "racesuit": racesuit, "shoes": shoes,
                      "bindings": bindings, "skipoles": skipoles, "gloves": gloves,
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
        if connection:
            connection.close()
            print("The sqlite connection is closed")
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
    print("Please try to use same names for same/similar things")
    birthdate_inp = input('Please enter the birthdate (in YYYY-MM-DD as string): ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')
    hobbies_inp = input(languages, '\nPlease enter the language of the athlete: ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')
    nationality_inp = input(country, '\nPlease enter the nationality of the athlete: ')

    languages_inp = input('Please enter the languages the athlete speaks: ')
    hobbies = input('Please enter the hobbies: ')
        # hobbies += '[' + ']'
        # profession = input('Please enter the profession: ')
        # family = input('Please enter the family: ')
        # datenbank überprüfen ob so ein name mit land schon existiert
        # w
        # pass
        # cursor.execute("""
        #               INSERT INTO Athlete
        #                     VALUES (?,?,?,?,?,?,?)
        #            """,
        #          (name, birthdate, nationality, languages, hobbies, profession, family)
        #         )




    sql_insert = """INSERT INTO Athlete 
                    VALUES
                    (name, birthdate, nationality, languages, hobbies, profession, family, skis, rifle, 
                    ammunition, racesuit, shoes, bindings, skipoles, gloves, wax, goggles);
                """




def update_athlete_db(text_ls):
    """"""
    for text in text_ls:
        athlete_names = get_all_athletes()

        with open('Data/blacklist.json', 'r') as file:
            blacklist_dict = json.loads(file.read())
            no_names = blacklist_dict["no_names"]
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
                blacklist_dict = {"no_names": no_names, "languages": languages, "hobbies": hobbies,
                                  "profession": profession, "family": family, "skis": skis,
                                  "rifle": rifle, "ammunition": ammunition, "racesuit": racesuit,
                                  "shoes": shoes, "bindings": bindings, "skipoles": skipoles,
                                  "gloves": gloves, "wax": wax, "goggles": goggles}
                json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))
            else:
                continue