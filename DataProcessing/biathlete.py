import re
import sqlite3
from DataProcessing import database_connection as dc


def get_all_athletes():
    """This method gets the name of the athletes from the table 'ATHLETE' of the database."""

    connection = dc.get_connection()
    try:
        cursor = connection.cursor()
        sql_names_country = "SELECT name FROM ATHLETE"
        cursor.execute(sql_names_country)
        rows = cursor.fetchall()
        name_column = []
        for row in rows:
            name_column.append(row[0])
        return name_column
    except sqlite3.Error as error:
        print("Failed to read data from table Athlete", error)
        return None


# This method can be used to do sql injections. It's the users responsibility to not do this.
def create_athlete(athlete_name):
    """This method creates a new athlete tuple/row in the database (table = Athlete).

        The data is read from the console which is entered by the user.
        :arg:
            athlete_name (str): name of the athlete
    """
    if not isinstance(athlete_name, str):
        raise TypeError

    print("This is the athlete's name we want to create: ", athlete_name)
    # json values
    try:
        no_names, country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit, \
        shoes, bindings, skipoles, gloves, wax, goggles = dc.get_json_lists()
    except TypeError:
        print("This should work to create an athlete. Please look into converting_data.get_json_lists()")
        return

    # input of an athlete's data
    print("Please insert STOP if you misspelled an input before")
    print("Please try to use same names for same/similar things. If data is not existing use NULL")

    birthdate_inp = input('Please enter the birthdate (in YYYY-MM-DD as string): ')
    pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    if (not pattern.match(birthdate_inp)) and (not birthdate_inp == 'NULL'):
        print("Wrong birthday input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

    print("list of countries: ", country)
    country_inp = input('Please enter the country of the athlete: ')
    pattern = re.compile(r"\w{3}")
    if (not pattern.match(country_inp)) and (not country_inp == 'NULL'):
        print("Wrong country input! Input has to have three characters. Please try again.")
        create_athlete(athlete_name)  # try again
        return

    print("list of languages: ", languages)
    languages_inp = []
    while True:
        another_language = input("Another language? (=y for YES). If NULL, press 'y'.: ")
        if another_language == "y":
            languages_inp.append(input('Please enter another language of the athlete: '))
        else:
            break
    if not languages_inp:
        languages_inp.append('NULL')

    print("list of hobbies: ", hobbies)
    hobbies_inp = []
    while True:
        another_hobby = input("Another hobby? (=y for YES). If NULL, press 'y'.: ")
        if another_hobby == "y":
            hobbies_inp.append(input('Please enter another hobby of the athlete: '))
        else:
            break
    if not hobbies_inp:
        hobbies_inp.append('NULL')

    birthplace_inp = input('Please enter the birthplace of the athlete: ')
    pattern = re.compile(r"\w{1,50}")
    if (not pattern.match(birthplace_inp)) and (not birthplace_inp == 'NULL'):
        print("Invalid birthplace input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

    residence_inp = input('Please enter the residence of the athlete: ')
    pattern = re.compile(r"\w{1,50}")
    if (not pattern.match(residence_inp)) and (not residence_inp == 'NULL'):
        print("Invalid residence input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

    print("list of professions: ", profession)
    profession_inp = input('Please enter the profession of the athlete: ')
    pattern = re.compile(r"\w{1,30}")
    if (not pattern.match(profession_inp)) and (not profession_inp == 'NULL'):
        print("Invalid profession input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

    print("list of family types: ", family)
    family_inp = input('Please enter the family status of the athlete: ')
    pattern = re.compile(r"\w{1,20}")
    if (not pattern.match(family_inp)) and (not family_inp == 'NULL'):
        print("Invalid family input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

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
        size_inp = input('Please enter the size of the athlete. If not available insert -1: ')
        weight_inp = input('Please enter the weight of the athlete. If not available insert -1: ')
        try:
            size_inp = int(size_inp)
            weight_inp = int(weight_inp)
            break
        except ValueError:
            print("Size and weight have to be integers! Please try again.")
            create_athlete(athlete_name)  # try again
            return

    gender_inp = input('Please enter the gender (1 = Male, 0 = Female) of the athlete: ')
    try:
        int(gender_inp)
    except ValueError:
        print("Gender has to be an integer! Please try again.")
        create_athlete(athlete_name)
        return

    died_inp = input('If dead please enter date, otherwise NULL: ')
    pattern = re.compile(r"\d{4}[-]\d{2}[-]\d{2}")
    if (not pattern.match(died_inp)) and (not died_inp == 'NULL'):
        print("Wrong date of death input! Please try again.")
        create_athlete(athlete_name)  # try again
        return

    cause_of_death_inp = input('Please enter the cause_of_death which of the athlete: ')

    current_ls = [country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit,
                  shoes, bindings, skipoles, gloves, wax, goggles]
    update_ls = [country_inp, languages_inp, hobbies_inp, profession_inp,
                 family_inp, skis_inp, rifle_inp, ammunition_inp, racesuit_inp, shoes_inp, bindings_inp,
                 skipoles_inp, gloves_inp, wax_inp, goggles_inp]

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
    dc.set_json_lists(current_ls)

    # insert into database
    insert_tuple = (athlete_name, birthdate_inp, country_inp, str(languages_inp), str(hobbies_inp),
                    profession_inp, family_inp, skis_inp, rifle_inp, ammunition_inp,
                    racesuit_inp, shoes_inp, bindings_inp, skipoles_inp, gloves_inp, wax_inp, goggles_inp,
                    size_inp, weight_inp, gender_inp, birthplace_inp, residence_inp, died_inp,
                    cause_of_death_inp)

    connection = dc.get_connection()
    sql_insert = """INSERT INTO Athlete(name, birthdate, country, languages, hobbies, profession, family,
                     skis, rifle, ammunition, racesuit, shoes, bindings, skipoles, gloves, wax, goggles,
                     size, weight, gender, birthplace, residence, died, cause_of_death)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor = connection.cursor()
    cursor.execute(sql_insert, insert_tuple)
    connection.commit()
    print("Successfully added a new athlete.")


def update_athlete_db(text_ls):
    """This method decides for every string in a document if it's a name of an athlete.

        :arg:
            text_ls (str list): text of a pdf document
        :return:
            index_list (integer list): indices in text_ls which point to a name of a biathlete
    """
    if not isinstance(text_ls, list):
        raise TypeError
    for j in text_ls:
        if not isinstance(j, str):
            raise TypeError
    index_list = []
    connection = dc.get_connection()

    for index, text in enumerate(text_ls):
        # json values
        no_names, country, languages, hobbies, profession, family, skis, rifle, ammunition, racesuit, \
        shoes, bindings, skipoles, gloves, wax, goggles = dc.get_json_lists()

        # if text is in the list of no_names text can be skipped
        if text in no_names:
            continue

        # !!! Athlete with same names will have the same "ID". We will separate two different athletes
        # with the same name later through tests.
        # This could cause a mistake in the database !!!!!!!!!!!!!
        # name is already in database
        athlete_names = get_all_athletes()
        text = text.replace("-", " ")
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
                dc.set_json_lists(current_ls)
    connection.close()
    return index_list
