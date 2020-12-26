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
        goggles VARCHAR(20)
        
        PRIMARY KEY(name, birthdate, country)
        );"""
    cursor.execute(creation_sql)
    connection.commit()
    connection.close()


    # create a json file with empty lists
    no_names = languages = hobbies = profession = family = skis = rifle = ammunition = \
        racesuit = shoes = bindings = skipoles = gloves = wax = goggles = []
    blacklist_dict = {"no_names": no_names, "languages": languages, "hobbies": hobbies,
                      "profession": profession, "family": family, "skis": skis, "rifle": rifle,
                        "ammunition": ammunition, "racesuit": racesuit, "shoes": shoes,
                        "bindings": bindings, "skipoles": skipoles, "gloves": gloves,
                        "wax": wax, "goggles": goggles}
    json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))


def existing_biathlete(name, country, text_ls):
    """"""
    for index, text in enumerate(text_ls):
        connection = sqlite3.connect("Data/Biathlon_Data.db")
        cursor = connection.cursor()
        sql_names_country = "SELECT name, country FROM Athlete"
        cursor.execute(sql_names_country)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        connection.close()

        #for sql_index, row_name in enumerate(row[0]):
         #   if name ==
          #     if text_ls[index +1] == rows[0][sql_index]:
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

        for j in text: # go to next element in text_ls because a name of an athlete has not integers
            if j.isdigit():
                break
        else:
            inp = input("Please decide if this is a name(=Y for YES) or not (=N for NO): ")
            if inp == "Y":
                pass
                # look into sql database if name already exists
                #if exists:
                 #   self.data['Name'] = name  # also name und land zuweisen
                  #  sel.data[country] = -...
                #else:
                 #   name = input('Please enter the name: ')
                  #  birthdate = input('Please enter the birthdate: ')
                   # nationality = input('Please enter the nationality of the athlete: ')
                    #languages = input('Please enter the languages: ')
                    #languages += '[' + ']'
                    #hobbies = input('Please enter the hobbies: ')
                    #hobbies += '[' + ']'
                    #profession = input('Please enter the profession: ')
                    #family = input('Please enter the family: ')
                    # datenbank überprüfen ob so ein name mit land schon existiert
                    # w
                    #pass
                cursor.execute("""
                                INSERT INTO Athlete 
                                       VALUES (?,?,?,?,?,?,?)
                               """,
                               (name, birthdate, nationality, languages, hobbies, profession, family)
                               )


            else:
                no_names.append(text)
                blacklist_dict = {"no_names": no_names, "languages": languages, "hobbies": hobbies,
                                  "profession": profession, "family": family}
                json.dump(blacklist_dict, open('Data/blacklist.json', 'w'))





def update_db():
    pass


class Biathlete:
    def __init__(self, name, country, birthdate):
        self.name = name
        self.birthdate = birthdate
        self.country = country
        # test if a biathlete with the same name, country and birthdate exist
        self.update_biathlete(self.country,self.birthdate)


    def update_biathlete(self, country, birthdate):
        inp = input("Please check if this biathlete has changed his name or coincidentally"
              "has the same birthdate and country of origin. YES(Y) = other biathlete"
              "NO(N) = same biathlete who has changed his name:")
        if inp == 'Y':
            pass

