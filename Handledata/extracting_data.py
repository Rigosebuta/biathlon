""" This module extract data from the 'Document' objects"""

from Handledata import converting_data as cv
import re
import datetime
import numpy as np
import pandas as pd


class BiathlonData:
    """This class assigns to every pdf document data from the pdf document"""

    def __init__(self, pdf_doc, organisation):
        """ declaration of variables which will then be filled with the information assigned to the pdf

        Args:
            organisation(str): we can't get this information out of the pdf so we have to sort
            the pdf to organisation groups before we extract data
            pdf_doc (Document): a 'Document' object

        Detailed description of metadata
                -'place': place where race take place

                -'place_country': country of the place

                -'age_group': age_group of the biathletes (youth, junior or senior)

                -'organisation': describes the organisation and particularly the competition level

                -'description': describes the type of content of the document
                    (e.g. Competition Analysis, Start List, Data Summary)

                -'race_type': race_type of the document (e.g. Mass Start, Sprint, Relay or Pursuit,...)

                -'race_len_km': race length of the document (10 km, 15 km, ....)

                -'date': creation date of the pdf document

                -'ranked': number of athletes who ranked in the race
                => this should be equal to (number of entries) - (did not start) - (did not finish)

                -'weather': weather conditions at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'snow_condition': snow conditions at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'snow_temperature': in celsius; snow temperature at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'air_temperature': in celsius; air temperature at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'humidity': in percent; humidity at different times
                  =>[30 min. before Start, At Start T2ime, 30 min. after Start, At End Time]

                -'wind_direction': wind directions at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'wind_speed': in meter per second; wind speed at different times
                  =>[30 min. before Start, At Start Time, 30 min. after Start, At End Time]

                -'level_difficulty': describes the level of difficulty of the race track

                -'total_course_length':  in meter

                -'height_difference': in meter

                -'max_climb': in meter

                -'total_climb': in meter
        """
        self.organisation = organisation
        self.pdf_doc = pdf_doc

        self.metadata = {'place': None, 'place_country': None, 'age_group': None, 'organisation': None,
                         'description': None, 'gender': None, 'race_type': None, 'race_len_km': None,
                         'date': None, 'number_of_entries': None, 'did_not_start': None,
                         'did_not_finish': None, 'lapped': None, 'disqualified': None,
                         'disqualified_for_unsportsmanlike_behaviour': None, 'ranked': None,
                         'weather': None, 'snow_condition': None, 'snow_temperature': None,
                         'air_temperature': None, 'humidity': None, 'wind_direction': None,
                         'wind_speed': None, 'total_course_length': None, 'height_difference': None,
                         'max_climb': None, 'total_climb': None, 'level_difficulty': None}
        self.get_metadata()

        self.data = None
        if self.metadata['description'] == "COMPETITION ANALYSIS":
            self.get_data()

        self.start_list = None
        if self.metadata['description'] == "START LIST":
            self.get_start_list()

    def get_metadata(self):
        """This method extracts metadata from the document

        """
        # get first page of a pdf doc => first page is enough to find out some of the metadata
        page_zero = self.pdf_doc.loadPage(0)
        text_zero = page_zero.getText('text')

        # get all lines of the pdf as a string list
        pages = cv.divide_into_pages(self.pdf_doc)
        text = ""
        for i in pages:
            text = text + i.getText('text')
        text_ls = text.split("\n")

        # search for description
        description_d = ['Competition Analysis', 'COMPETITION ANALYSIS', 'Competition TestData Summary',
                         'COMPETITION DATA SUMMARY', 'Start List', 'START LIST']
        self.get_basic_metadata(text_zero, description_d, 'description')

        # search for place
        # get first page of a pdf doc => first page is enough to find out the place
        self.get_place(text_zero)

        # search for country of place
        # to work properly place has to be determined before this
        self.get_place_country(self.metadata['place'])

        # search for organisation
        self.get_organisation()

        # search for age
        age_group_d = ['YOUTH', 'JUNIOR']
        self.get_basic_metadata(text_zero, age_group_d, 'age_group')

        # search for gender
        gender_d = ['WOMEN', 'MEN']
        self.get_basic_metadata(text_zero, gender_d, 'gender')

        # search for race_type
        race_type_d = ['SPRINT', 'PURSUIT', 'RELAY', 'INDIVIDUAL', 'MASS START', 'MIXED']
        self.get_basic_metadata(text_zero, race_type_d, 'race_type')

        # search for race_length
        self.get_race_len(text_zero)

        # search for date
        self.get_date()

        if self.metadata['description'] == 'COMPETITION DATA SUMMARY':
            # get number of entries
            self.get_some_data_summary(text_ls, 'Number of Entries')

            # get number of did_not_start
            self.get_some_data_summary(text_ls, 'Did not start')

            # get number of did_not_finish
            self.get_some_data_summary(text_ls, 'Did not finish')

            # get number of lapped biathletes
            self.get_some_data_summary(text_ls, 'Lapped')

            # get number of disqualified biathletes
            self.get_some_data_summary(text_ls, 'Disqualified')

            # get number of disqualified_for_unsportsmanlike_behaviour
            self.get_some_data_summary(text_ls, 'Disqualified for unsportsmanlike behaviour')

            # get number of ranked competitors
            self.get_some_data_summary(text_ls, 'Ranked')

            # get total length of the course
            self.get_some_data_summary(text_ls, 'Total Course Length')

            # get height difference of the course
            self.get_some_data_summary(text_ls, 'Height Difference')

            # get maximum climb
            self.get_some_data_summary(text_ls, 'Max. Climb')

            # get total climb
            self.get_some_data_summary(text_ls, 'Total Climb')

            # get level of difficulty
            level_difficulty_d = ['red', 'blue', 'green']
            self.get_basic_metadata(text, level_difficulty_d, 'level_difficulty')

            # get weather conditions (list)
            self.get_weather(text_ls)

            # get snow condition (list)
            self.get_different_weather_conditions(text_ls, 'Snow Condition')

            # get snow temperature
            self.get_different_weather_conditions(text_ls, 'Snow Temperature')

            # get air_temperature
            self.get_different_weather_conditions(text_ls, 'Air Temperature')

            # get humidity
            self.get_different_weather_conditions(text_ls, 'Humidity')

            # get wind_speed and wind direction
            self.get_different_weather_conditions(text_ls, 'Wind Direction/Speed')

        print(self.metadata)

    def get_place(self, text_zero):
        """This method searches in document for one of the following places

            Args:
                text_zero (str): first page of the document as a string

            - If a place matches the string, metadata['place'] will be updated
            - If no place matches the string, it will be raised an exception
        """

        # all possible places in Biathlon
        place_d = ['ALTENBERG', 'BAYERISCH EISENSTEIN', 'CLAUSTHAL-ZELLERFELD',
                   'GELSENKIRCHEN', 'KALTENBRUNN', 'NOTSCHREI', 'OBERHOF',
                   'OBERWIESENTHAL', 'RUHPOLDING', 'WILLINGEN', 'BEITOSTOLEN',
                   'GEILO', 'LILLEHAMMER', 'OSLO', 'SJUSJOEN', 'TRONDHEIM', 'ALMATY',
                   'ANTHOLZ', 'BRUSSON', 'CESANA-SAN SICARIO', 'FORNI AVOLTRI', 'MARTELL', 'RIDNAUN',
                   'ANNECY', 'HAUTE-MAURIENNE', 'LES SAISIES', 'PREMANON', 'BANSKO', 'CANMORE',
                   'LA PATRIE', 'VALCARTIER', 'WHISTLER', 'CHANTY-MANSIJSK', 'ISCHEWSK',
                   'KRASNAJA POLJANA', 'NOWOSIBIRSK', 'TJUMEN', 'UFA', 'UWAT', 'CHEILE GRADISTEI',
                   'DUSZNIKI-ZDROJ', 'ZAKOPANE-KOSCIELISKO', 'ERZURUM', 'FORT KENT', 'ITASCA',
                   'JERICHO', 'LAKE PLACID', 'PRESQUE ISLE', 'SALT LAKE CITY', 'WEST YELLOWSTONE',
                   'HOCHFILZEN', 'OBERTILLIACH', 'WINDISCHGARSTEN', 'IDRE', 'ÖSTERSUND', 'TORSBY',
                   'JABLONEC NAD NISOU', 'NOVE MESTO', 'KONTIOLAHTI', 'LAHTI', 'LANTSCH', 'LENZ',
                   'MINSK', 'NOZAWA ONSEN', 'OSRBLIE', 'OTEPÄÄ', 'POKLJUKA', 'PYEONGCHANG',
                   'SOLDIER HOLLOW', 'HOLMENKOLLEN', 'SOLDIER HOLLOW', 'WHISTLER OLYMPIC PARK',
                   'Kiry Biathlon Stadium', 'MERIBEL', 'GURNIGEL', 'KHANTY-MANSIYSK',
                   'Liatoppen Skisenter', 'LIATOPPEN SKISENTER', 'KOSCIELISKO',
                   'BIATHLON STADION AM GRENZADLER']
        place_cap = [k.capitalize() for k in place_d]
        place_more = ['Salt Lake City', 'Lake Placid', 'Duszniki-Zdroj', 'Haute-Maurienne',
                      'Bayerisch Eisenstein', 'Clausthal-Zellerfeld', 'Cesana-San Sicario',
                      'Forni Avoltri', 'Les Saisies', 'La Patrie', 'Chanty-Mansijsk',
                      'Krasnaja Poljana', 'Cheile Gradistei', 'Zakopane-Koscielisko',
                      'West Yellowstone', 'Jablonec Nad Nisou', 'Nove Mesto', 'Nozawa Onsen',
                      'Soldier Hollow', 'Whistler Olympic Park', 'Khanty-Mansiysk']
        place_d += place_cap
        place_d += place_more

        # search for place in text
        for pl in place_d:
            match = re.search(pl, text_zero)
            if match:
                pl = pl.upper()

                # rewrite the location, some places may have several names in use
                if pl == 'OSLO':  # == compare on value not on object id
                    pl = 'HOLMENKOLLEN'
                elif pl == 'SALT LIKE CITY':
                    pl = 'SOLDIER HOLLOW'
                elif pl == 'LIATOPPEN SKISENTER':
                    pl = 'AL'
                elif pl == 'KIRY BIATHLON STADIUM' or pl == 'KOSCIELISKO':
                    pl = 'ZAKOPANE-KOSCIELISKO'
                elif pl == 'BREZNO_OSRBLIE':
                    pl = 'OSRBLIE'
                elif pl == 'BIATHLON STADION AM GRENZADLER':
                    pl = 'OBERHOF'
                self.metadata['place'] = pl
                break
        else:  # gets only executed if no match is found
            print('Please update the places or insert it manual', self.pdf_doc)
            # print(text_zero)  # if looking in the text is necessary
            # os.startfile(self.pdf_doc.name)
            # inp = input("Please update/enter the place manual:")
            # self.get_place(inp)

    def get_place_country(self, place):
        """This method assigns the country to the place

            Args:
                place (str): place where race take place
        """

        place_country = None
        if place in ['ALTENBERG', 'BAYERISCH EISENSTEIN', 'CLAUSTHAL-ZELLERFELD',
                     'GELSENKIRCHEN', 'KALTENBRUNN', 'NOTSCHREI', 'OBERHOF',
                     'OBERWIESENTHAL', 'RUHPOLDING', 'WILLINGEN']:
            place_country = 'GER'
        elif place in ['AL', 'BEITOSTOLEN', 'GEILO', 'LILLEHAMMER', 'HOLMENKOLLEN', 'SJUSJOEN', 'TRONDHEIM']:
            place_country = 'NOR'
        elif place in ['ALMATY']:
            place_country = 'KAZ'
        elif place in ['ANTHOLZ', 'BRUSSON', 'CESANA-SAN SICARIO', 'FORNI AVOLTRI', 'MARTELL', 'RIDNAUN']:
            place_country = 'ITA'
        elif place in ['ANNECY', 'HAUTE-MAURIENNE', 'LES SAISIES', 'PREMANON']:
            place_country = 'FRA'
        elif place in ['BANSKO']:
            place_country = 'BGR'
        elif place in ['CANMORE', 'LA PATRIE', 'VALCARTIER', 'WHISTLER']:
            place_country = 'CAN'
        elif place in ['CHANTY-MANSIJSK', 'ISCHEWSK', 'KRASNAJA POLJANA', 'NOWOSIBIRSK', 'TJUMEN', 'UFA', 'UWAT']:
            place_country = 'RUS'
        elif place in ['CHEILE GRADISTEI']:
            place_country = 'ROU'
        elif place in ['DUSZNIKI-ZDROJ', 'ZAKOPANE-KOSCIELISKO']:
            place_country = 'PL'
        elif place in ['ERZURUM']:
            place_country = 'TUR'
        elif place in ['FORT KENT', 'ITASCA', 'JERICHO', 'LAKE PLACID', 'PRESQUE ISLE',
                       'SOLDIER HOLLOW', 'WEST YELLOWSTONE']:
            place_country = 'USA'
        elif place in ['HOCHFILZEN', 'OBERTILLIACH', 'WINDISCHGARSTEN']:
            place_country = 'AUT'
        elif place in ['IDRE', 'ÖSTERSUND', 'TORSBY']:
            place_country = 'SWE'
        elif place in ['JABLONEC NAD NISOU', 'NOVE MESTO']:
            place_country = 'CZE'
        elif place in ['KONTIOLAHTI', 'LAHTI']:
            place_country = 'FIN'
        elif place in ['LANTSCH', 'LENZ']:
            place_country = 'CHE'
        elif place in ['MINSK']:
            place_country = 'BLR'
        elif place in ['NOZAWA ONSEN']:
            place_country = 'JPN'
        elif place in ['OSRBLIE']:
            place_country = 'SVK'
        elif place in ['OTEPÄÄ']:
            place_country = 'EST'
        elif place in ['POKLJUKA']:
            place_country = 'SVN'
        elif place in ['PYEONGCHANG']:
            place_country = 'KOR'
        else:
            print('Please update the countries / the method. Place could not be assigned to any country')

        self.metadata['place_country'] = place_country

    def get_basic_metadata(self, text_zero, ls, key):
        """This method searches in document for basic metadata

            Args:
                text_zero (str): first page of the document as a string
                ls (str list): possible matches in text_zero
                key (str): attribute in self.metadata which will be updated
        """
        for elem in ls:
            match = re.search(elem, text_zero)
            if match:
                elem = elem.upper()
                self.metadata[key] = elem
                break
        else:
            if key == 'age_group':
                self.metadata['age_group'] = 'SENIOR'
            elif key == 'description':
                print('Please update the description list in get_metadata()', self.pdf_doc)
            elif key == 'gender':
                print('Please look into the gender. Something is really wrong', self.pdf_doc)
            elif key == 'race_type':
                print('Please update the race_type list in get_metadata()', self.pdf_doc)
            elif key == 'level_difficulty':
                print('Please update the level list', self.pdf_doc)

    def get_organisation(self):
        """This method searches a list for the organisation"""

        organisation_d = ['IBU CUP', 'WORLD CUP', 'OLYMPIC GAMES',
                          'WORLD CHAMPIONSHIPS', 'IBU JUNIOR CUP']
        for org in organisation_d:
            if org == self.organisation:
                self.metadata['organisation'] = org
                break
        else:
            print('Please update the organisation list', self.pdf_doc)

    def get_race_len(self, text_zero):
        """This method searches in document for the race_type

            Args:
                text_zero (str): first page of the document as a string
        """

        race_len_km_d = ['3 X 7.5 km', '4 X 7.5 km', '3 X 6 km', '4 X 6 km',
                         '3x7.5 km', '4x7.5 km', '3x6 km', '4x6 km',
                         '3x7.5 KM', '4x7.5 KM', '3x6 KM', '4x6 KM',
                         '3X7.5 km', '4X7.5 km', '3X6 km', '4X6 km', '10 km',
                         '15 km', '20 km', '12.5 km', '7.5 km', '6 km']

        race_len_km_d += [rlk.upper() for rlk in race_len_km_d]
        race_len_km_d += [rl.replace(" ", "") for rl in race_len_km_d]
        for race_length in race_len_km_d:
            match = re.search(race_length, text_zero)
            if match:
                race_length = race_length.upper()
                race_length = race_length.replace(" ", "")
                #                if self.metadata['race_type'] == 'RELAY' and ('X' not in race_length):
                #                   raise Exception  # !!!!!!!!!!!!!!!!!!!
                self.metadata['race_len_km'] = race_length
                break
        if self.metadata['race_len_km'] is None:
            print('Please update the race_len in km list', self.pdf_doc)

    def get_date(self):
        """This method extracts the date of the race through the pdfs metadata"""
        data_pdf = self.pdf_doc.metadata
        datum = data_pdf['creationDate']
        try:
            year = int(datum[2:6])
            month = int(datum[6:8])
            day = int(datum[8:10])
            self.metadata['date'] = datetime.date(year, month, day)
        except TypeError:
            print('This should not happen. Please check the method get_date')

    def get_some_data_summary(self, text_ls, key):
        """This method searches for the key in the text (document) and
            considers the value in the next line

            Args:
                text_ls (str): text of the document as a string list
                key (str): information which is searched for
        """
        number = -1
        for i, j in enumerate(text_ls):
            if key in j:
                try:
                    number_text = text_ls[i + 1]
                    number_text = "".join([digit for digit in number_text if digit.isdigit()])
                    number = int(number_text)
                    break
                except ValueError:
                    print('the rounding of number entries failed')
                except IndexError:
                    print('this should not happen; Please look into get_some_data_summary')
        if not (number == -1):
            if key == 'Number of Entries':
                self.metadata['number_of_entries'] = number
            elif key == 'Did not start':
                self.metadata['did_not_start'] = number
            elif key == 'Did not finish':
                self.metadata['did_not_finish'] = number
            elif key == 'Lapped':
                self.metadata['lapped'] = number
            elif key == 'Disqualified':
                self.metadata['disqualified'] = number
            elif key == 'Disqualified for unsportsmanlike behaviour':
                self.metadata['disqualified_for_unsportsmanlike_behaviour'] = number
            elif key == 'Ranked':
                self.metadata['ranked'] = number
            elif key == 'Total Course Length':
                self.metadata['total_course_length'] = number
            elif key == 'Height Difference':
                self.metadata['height_difference'] = number
            elif key == 'Max. Climb':
                self.metadata['max_climb'] = number
            elif key == 'Total Climb':
                self.metadata['total_climb'] = number

    def get_weather(self, text_ls):
        """This method extracts the weather conditions from the data (pdf file)

            Args:
                text_ls (str): text of the document as a string list

        """
        for i, j in enumerate(text_ls):
            try:
                if 'End Time' in j and 'Weather' in text_ls[i + 1]:
                    weather_ls = [None, None, None, None]
                    for k in range(i + 2, i + 6):
                        weather_ls[k - i - 2] = text_ls[k]
                    if None in weather_ls:
                        print('Please update get_weather()')
                        break
                    self.metadata['weather'] = weather_ls
                    break
            except IndexError:
                print('this should not happen; Please look into get_weather()')

    def get_different_weather_conditions(self, text_ls, key):
        """This method extracts additional weather conditions from the data (pdf file)

            Args:
                text_ls (str): text of the document as a string list
                key (str): information which is searched for
        """
        weather_ls = [None, None, None, None]
        for i, j in enumerate(text_ls):
            if key in j:
                try:
                    for k in range(i + 1, i + 5):
                        weather_ls[k - i - 1] = text_ls[k]
                    break
                except IndexError:
                    print('this should not happen; Please look into get_different_weather_conditions()')
        # print(weather_ls)
        if None not in weather_ls:
            if key == 'Snow Condition':
                self.metadata['snow_condition'] = weather_ls
            elif key == 'Snow Temperature':
                snow_temp = cv.from_celsius_to_float(weather_ls)
                self.metadata['snow_temperature'] = snow_temp
            elif key == 'Air Temperature':
                air_temp = cv.from_celsius_to_float(weather_ls)
                self.metadata['air_temperature'] = air_temp
            elif key == 'Humidity':
                humid = []
                for hum in weather_ls:
                    hum_without_perc = "".join([d for d in hum if d.isdigit()])
                    humid.append(int(hum_without_perc))
                self.metadata['humidity'] = humid
            elif key == 'Wind Direction/Speed':

                # Wind Direction
                first_two_letter = []
                for m in weather_ls:
                    first_two_letter.append(m[0:2])
                    if not m[0].isalpha():
                        break
                else:
                    wind_direction = [p.replace(" ", "") for p in first_two_letter]
                    self.metadata['wind_direction'] = wind_direction

                # Wind Speed
                wind_speed = cv.from_meter_per_s_to_float(weather_ls)
                self.metadata['wind_speed'] = wind_speed

    def get_data(self):
        """This method extracts specific race data from every biathlete (e.g. interim times, loop times)"""
        pages = cv.divide_into_pages(self.pdf_doc)
        text = ""
        for i in pages:
            text = text + i.getText('text')
        text_ls = text.split("\n")  # array of the lines of the string
        # print(text_ls)
        number_of_biathletes = 200
        if not self.metadata['number_of_entries'] is None:
            number_of_biathletes = self.metadata['number_of_entries']

        columns_list = ['Name', 'Rank', 'Total_Misses', 'Overall_Time', 'Overall_Time_Behind',
                        'Overall_Rank', 'Cumulative_Time_Loop1', 'Cumulative_Time_Loop1_Behind',
                        'Cumulative_Time_Loop1_Rank', 'Cumulative_Time_Loop2',
                        'Cumulative_Time_Loop2_Behind', 'Cumulative_Time_Loop2_Rank',
                        'Cumulative_Time_Loop3', 'Cumulative_Time_Loop3_Behind', 'Cumulative_Time_Loop3_Rank',
                        'Cumulative_Time_Loop4', 'Cumulative_Time_Loop4_Behind', 'Cumulative_Time_Loop4_Rank',
                        'Cumulative_Time_Overall', 'Cumulative_Time_Overall_Behind',
                        'Cumulative_Time_Overall_Rank', 'Loop_Time_Loop1', 'Loop_Time_Loop1_Behind',
                        'Loop_Time_Loop1_Rank', 'Loop_Time_Loop2', 'Loop_Time_Loop2_Behind',
                        'Loop_Time_Loop2_Rank', 'Loop_Time_Loop3', 'Loop_Time_Loop3_Behind',
                        'Loop_Time_Loop3_Rank', 'Loop_Time_Loop4', 'Loop_Time_Loop4_Behind',
                        'Loop_Time_Loop4_Rank', 'Loop_Time_Lap5', 'Loop_Time_Lap5_Behind',
                        'Loop_Time_Lap5_Rank', 'Shooting_Misses_Loop1', 'Shooting_Time_Loop1',
                        'Shooting_Time_Loop1_Behind', 'Shooting_Loop1_Rank', 'Shooting_Misses_Loop2',
                        'Shooting_Time_Loop2', 'Shooting_Time_Loop2_Behind', 'Shooting_Loop2_Rank',
                        'Shooting_Misses_Loop3', 'Shooting_Time_Loop3', 'Shooting_Time_Loop3_Behind',
                        'Shooting_Loop3_Rank', 'Shooting_Misses_Loop4', 'Shooting_Time_Loop4',
                        'Shooting_Time_Loop4_Behind', 'Shooting_Loop4_Rank', 'Shooting_Misses_Overall',
                        'Shooting_Time_Overall', 'Shooting_Time_Overall_Behind',
                        'Shooting_Overall_Rank', 'Range_Time_Loop1', 'Range_Time_Loop1_Behind',
                        'Range_Time_Loop1_Rank', 'Range_Time_Loop2', 'Range_Time_Loop2_Behind',
                        'Range_Time_Loop2_Rank', 'Range_Time_Loop3', 'Range_Time_Loop3_Behind',
                        'Range_Time_Loop3_Rank', 'Range_Time_Loop4', 'Range_Time_Loop4_Behind',
                        'Range_Time_Loop4_Rank', 'Range_Time_Overall', 'Range_Time_Overall_Behind',
                        'Range_Time_Overall_Rank', 'Course_Time_Loop1', 'Course_Time_Loop1_Behind',
                        'Course_Time_Loop1_Rank', 'Course_Time_Loop2', 'Course_Time_Loop2_Behind',
                        'Course_Time_Loop2_Rank', 'Course_Time_Loop3', 'Course_Time_Loop3_Behind',
                        'Course_Time_Loop3_Rank', 'Course_Time_Loop4', 'Course_Time_Loop4_Behind',
                        'Course_Time_Loop4_Rank', 'Course_Time_Lap5', 'Course_Time_Lap5_Behind',
                        'Course_Time_Lap5_Rank', 'Course_Time_Overall', 'Course_Time_Overall_Behind',
                        'Course_Time_Overall_Rank', 'Penalty_Time_Loop1', 'Penalty_Time_Loop2',
                        'Penalty_Time_Loop3', 'Penalty_Time_Loop4', 'Penalty_Time_Overall']

        self.data = pd.DataFrame(np.nan, index=list(range(number_of_biathletes)), columns=columns_list)

        types = [str, int, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp,
                 int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int,
                 pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp,
                 pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp,
                 int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, int,
                 pd.Timestamp, pd.Timestamp, int, int, pd.Timestamp, pd.Timestamp, int, int,
                 pd.Timestamp, pd.Timestamp, int, int, pd.Timestamp, pd.Timestamp, int, int,
                 pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp,
                 pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp,
                 int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int,
                 pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp,
                 pd.Timestamp, int, pd.Timestamp, pd.Timestamp, int, pd.Timestamp, pd.Timestamp,
                 int, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]

        if self.metadata['description'] == 'COMPETITION ANALYSIS':
            # print(self.data.index)
            # print(self.data.loc[0, ['Name']])  # selecting by label
            # print(self.data.iloc[3])    # selecting by position
            # print(self.data.iloc[3:5, 0:2])  # slicing
            # print(self.data.iloc[[1, 2, 4], [0, 2]])  # specific positions
            # print(self.data.iloc[1, 1])  # getting a value explicitly
            # print(self.data[self.data['Name'] > 0])  # boolean indexing
            # print(self.data[self.data > 0])
            # print(self.data[self.data['Name'].isin(['two', 'NaN'])])
            # print(self.data[self.data['Name'].isna()])


            self.data["Name"] = self.data["Name"].astype(str)
            print(text_ls)
            for i, j in enumerate(text_ls):
                try:
                    # name
                    first_name_rk = 'Rk.' in j and '1' in text_ls[i + 1]
                    first_name_behind = 'Behind Rank' in j and '1' in text_ls[i + 1]
                    if first_name_behind or first_name_rk:
                        name = text_ls[i + 3]
                        # print(name)
                        self.data.iat[0, 0] = name
                        break

                except IndexError:
                    print("look into get data")

            #                 country = text_ls[i+4]
            #                #self.data[1] = country
            #               total_misses = int(text_ls[i+5])
            #              #self.data[2] = total_misses
            #             time = cv.get_time(text_ls[i+6])
            #            #total_time =
            #           weather_ls = [None, None, None, None]
            # print(self.data.head())
        #for h, column in enumerate(columns_list):
         #   self.data[column] = self.data[column].astype(types[h])

    #  except IndexError:
    #     print('this should not happen; Please look into get_data()')
    # print(self.data)

    def get_start_list(self):
        self.start_list = 0
        pass  # update self.start_list
