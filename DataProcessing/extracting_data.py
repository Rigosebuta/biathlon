""" This module extract data from the 'Document' objects."""

import datetime
import os
import re
import numpy as np
import pandas as pd
import fitz
from DataProcessing import converting_data as cv, biathlete as ba


class BiathlonData:
    """This class assigns to every pdf document data from the pdf document."""

    def __init__(self, pdf_doc, organisation):
        """ Declaration of variables which will then be filled with the information assigned to the pdf.

        :arg:
            organisation(str): we can't get this information out of the pdf so we have to presort
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
                         'max_climb': None, 'total_climb': None, 'level_difficulty': None,
                         'race_start': None}
        self.skip_flag = False
        self.get_metadata()

        self.data = None
        if self.metadata['description'] == "COMPETITION ANALYSIS":
            self.get_data()

        self.start_list = None
        if self.metadata['description'] == "START LIST":
            self.get_start_list()
        elif self.metadata['race_type'] == 'MASS START':
            self.get_start_list()


    def get_metadata(self):
        """This method extracts metadata from the document."""

        # get first page of a pdf doc => first page is enough to find out some of the metadata
        page_zero = self.pdf_doc.loadPage(0)
        text_zero = page_zero.getText('text')
        text_zero_ls = text_zero.split("\n")

        # get all lines of the pdf as a string list
        all_pages = cv.divide_into_pages(self.pdf_doc)
        all_text = ""
        for p in all_pages:
            all_text = all_text + p.getText('text')
        all_text_ls = all_text.split("\n")

        # search for description
        description_d = ['Competition Analysis', 'COMPETITION ANALYSIS', 'Competition TestData Summary',
                         'COMPETITION DATA SUMMARY', 'Start List', 'START LIST']
        self.get_basic_metadata(text_zero, description_d, 'description')

        # search for place
        # first page is enough to find out the place
        self.get_place(text_zero_ls)

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
            self.get_some_data_summary(all_text_ls, 'Number of Entries')

            # get number of did_not_start
            self.get_some_data_summary(all_text_ls, 'Did not start')

            # get number of did_not_finish
            self.get_some_data_summary(all_text_ls, 'Did not finish')

            # get number of lapped biathletes
            self.get_some_data_summary(all_text_ls, 'Lapped')

            # get number of disqualified biathletes
            self.get_some_data_summary(all_text_ls, 'Disqualified')

            # get number of disqualified_for_unsportsmanlike_behaviour
            self.get_some_data_summary(all_text_ls, 'Disqualified for unsportsmanlike behaviour')

            # get number of ranked competitors
            self.get_some_data_summary(all_text_ls, 'Ranked')

            # get total length of the course
            self.get_some_data_summary(all_text_ls, 'Total Course Length')

            # get height difference of the course
            self.get_some_data_summary(all_text_ls, 'Height Difference')

            # get maximum climb
            self.get_some_data_summary(all_text_ls, 'Max. Climb')

            # get total climb
            self.get_some_data_summary(all_text_ls, 'Total Climb')

            # get level of difficulty
            level_difficulty_d = ['red', 'blue', 'green']
            self.get_basic_metadata(all_text, level_difficulty_d, 'level_difficulty')

            # get weather conditions (list)
            self.get_weather(all_text_ls)

            # get snow condition (list)
            self.get_different_weather_conditions(all_text_ls, 'Snow Condition')

            # get snow temperature
            self.get_different_weather_conditions(all_text_ls, 'Snow Temperature')

            # get air_temperature
            self.get_different_weather_conditions(all_text_ls, 'Air Temperature')

            # get humidity
            self.get_different_weather_conditions(all_text_ls, 'Humidity')

            # get wind_speed and wind direction
            self.get_different_weather_conditions(all_text_ls, 'Wind Direction/Speed')
        elif self.metadata['description'] == 'START LIST':
            # get race_start
            self.get_race_start(all_text_ls)

    def get_place(self, text_zero):
        """This method searches in document for one of the following places

            :arg:
                text_zero (str): first page of the document as a string

            - If a place matches the string, metadata['place'] will be updated
            - If no place matches the string, it will asked for an input of the user
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
            if pl in text_zero:
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
                elif pl == 'BREZNO-OSRBLIE':
                    pl = 'OSRBLIE'
                elif pl == 'BIATHLON STADION AM GRENZADLER':
                    pl = 'OBERHOF'
                self.metadata['place'] = pl
                break
        else:  # gets only executed if no match is found
            os.startfile(self.pdf_doc.name)
            inp = input("Please update/enter the place manual (in uppercase):")
            self.get_place(inp)

    def get_place_country(self, place):
        """This method assigns the country to the place

            :arg:
                place (str): place where race take place
        """

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
        elif place in ['KHANTY-MANSIYSK', 'CHANTY-MANSIJSK', 'ISCHEWSK', 'KRASNAJA POLJANA', 'NOWOSIBIRSK',
                       'TJUMEN', 'UFA', 'UWAT']:
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
            raise Exception('Please update the countries in the get_place_country().'
                            'The place could not be assigned to any country')

        self.metadata['place_country'] = place_country

    def get_basic_metadata(self, text_zero, ls, key):
        """This method searches in document for basic metadata

            :arg:
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
                print('Please update the description list in get_basic_metadata()', self.pdf_doc)
                # gets only executed if no match is found
                os.startfile(self.pdf_doc.name)
                desc_inp = input("Please update/enter the description (Competition Analysis, COMPETITION ANALYSIS, "
                                 "Competition TestData Summary, "
                                 "COMPETITION DATA SUMMARY, Start List, START LIST) manual (in uppercase):")
                self.get_basic_metadata(desc_inp, ls, key)
            elif key == 'gender':
                print('Please look into the gender and get_basic_metadata()', self.pdf_doc)
                # gets only executed if no match is found
                os.startfile(self.pdf_doc.name)
                gender_inp = input("Please update/enter the gender ('WOMEN', 'MEN') manual (in uppercase):")
                self.get_basic_metadata(gender_inp, ls, key)
            elif key == 'race_type':
                print('Please update the race_type list in get_metadata()', self.pdf_doc)
                # gets only executed if no match is found
                os.startfile(self.pdf_doc.name)
                race_type_inp = input("Please update/enter the race_type ('SPRINT', 'PURSUIT', 'RELAY', 'INDIVIDUAL', "
                                      "'MASS START', 'MIXED') manual (in uppercase):")
                self.get_basic_metadata(race_type_inp, ls, key)
            elif key == 'level_difficulty':
                print('Please update the level list', self.pdf_doc)
                # gets only executed if no match is found
                os.startfile(self.pdf_doc.name)
                level_inp = input("Please update/enter the level (red, blue, green) manual (in uppercase):")
                self.get_basic_metadata(level_inp, ls, key)

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

            :arg:
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
            os.startfile(self.pdf_doc.name)
            race_len_inp = input("Please update/enter the race length ('3 X 7.5 km', '4 X 7.5 km', '3 X 6 km', "
                                 "'4 X 6 km', '3x7.5 km', '4x7.5 km', '3x6 km', '4x6 km', '3x7.5 KM', '4x7.5 KM', "
                                 "'3x6 KM', '4x6 KM', '3X7.5 km', '4X7.5 km', '3X6 km', '4X6 km', '10 km', '15 km', "
                                 "'20 km', '12.5 km', '7.5 km', '6 km') manual (in uppercase):")
            self.get_race_len(race_len_inp)

    def get_date(self):
        """This method extracts the date of the race through the pdfs metadata"""
        data_pdf = self.pdf_doc.metadata
        #        print(data_pdf)
        datum = data_pdf['creationDate']
        try:
            year = int(datum[2:6])
            month = int(datum[6:8])
            day = int(datum[8:10])
            #            print(datetime.date(year, month, day))!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.metadata['date'] = datetime.date(year, month, day)
        except TypeError:
            date_inp = input('Please enter the date (in YYYY-MM-DD as a string): ')
            pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
            if not pattern.match(date_inp):
                raise ValueError('Date input had the wrong structure.')

    def get_some_data_summary(self, text_ls, key):
        """This method searches for the key in the text (document) and
            considers the value in the next line

            :arg:
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

            :arg:
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

            :arg:
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
                snow_temp = cv.from_one_unity_to_float(weather_ls, "°")
                self.metadata['snow_temperature'] = snow_temp
            elif key == 'Air Temperature':
                air_temp = cv.from_one_unity_to_float(weather_ls, "°")
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
                wind_speed = cv.from_one_unity_to_float(weather_ls, " ")
                self.metadata['wind_speed'] = wind_speed

    def get_race_start(self, all_text_ls):

        start_time_ls = ['START TIME']
        for st in start_time_ls:
            for pos, text in enumerate(all_text_ls):
                match = re.search(st, text)
                if match:
                    text_split = text.split(" ")[3]
                    self.metadata['race_start'] = text_split
                    break

    def sprint_data(self, text_ls, index_list, minimum_limit):
        """This method extracts specific race data from a biathlete of a SPRINT race (e.g. interim times)"""
        # divide into a index list of athletes who finished the race properly and those who didn't
        for k, index_athlete in enumerate(index_list):
            if index_athlete >= minimum_limit:
                in_evaluation = index_list[:k]
                break
        else:
            in_evaluation = index_list

        j = 0
        for elem in in_evaluation:
            # this try construct is enough because once it is in the wrong order there is following a
            # ValueError and data from the athlete is read until the error.
            try:
                # name
                name = text_ls[elem]
                self.data.iat[j, 0] = name

                # country
                country = text_ls[elem + 1]
                self.data.iat[j, 1] = country

                # total misses
                total_misses = text_ls[elem + 2]
                self.data.iat[j, 2] = int(total_misses)

                big_deficit_adjustment = 0

                # overall time
                big_deficit_adjustment += self.data_splitting(j, elem + 3 - big_deficit_adjustment, 3, text_ls)

                # cumulative time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 7 - big_deficit_adjustment, 6, text_ls)

                # cumulative time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 10 - big_deficit_adjustment, 9, text_ls)

                # cumulative time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 13 - big_deficit_adjustment, 18, text_ls)

                # loop time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 17 - big_deficit_adjustment, 21, text_ls)

                # loop time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 20 - big_deficit_adjustment, 24, text_ls)

                # loop time loop 3
                big_deficit_adjustment += self.data_splitting(j, elem + 23 - big_deficit_adjustment, 27, text_ls)

                # shooting misses loop 1
                shooting_misses_1 = text_ls[elem + 27]
                self.data.iat[j, 36] = int(shooting_misses_1)

                # shooting time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 28 - big_deficit_adjustment, 37, text_ls)

                # shooting misses loop 2
                shooting_misses_2 = text_ls[elem + 31]
                self.data.iat[j, 40] = int(shooting_misses_2)

                # shooting loop time 2
                big_deficit_adjustment += self.data_splitting(j, elem + 32 - big_deficit_adjustment, 41, text_ls)

                # shooting misses overall
                shooting_misses_overall = text_ls[elem + 35]
                self.data.iat[j, 52] = int(shooting_misses_overall)

                # shooting time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 36 - big_deficit_adjustment, 53, text_ls)

                # range time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 40 - big_deficit_adjustment, 56, text_ls)

                # range time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 43 - big_deficit_adjustment, 59, text_ls)

                # range time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 46 - big_deficit_adjustment, 68, text_ls)

                # course time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 50 - big_deficit_adjustment, 71, text_ls)

                # course time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 53 - big_deficit_adjustment, 74, text_ls)

                # course time loop 3
                big_deficit_adjustment += self.data_splitting(j, elem + 56 - big_deficit_adjustment, 77, text_ls)

                # course time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 59 - big_deficit_adjustment, 86, text_ls)

            except ValueError:
                print('Please insert the data for {} manual.'.format(text_ls[elem]))
                j += 1
                continue
            j += 1
        self.data = self.data.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)

    def data_splitting(self, j, position, column, text_ls):
        if text_ls[position].count(" ") == 2:
            overall, behind, rank = text_ls[position].split(" ")
            self.data.iat[j, column] = overall
            self.data.iat[j, column + 1] = behind
            self.data.iat[j, column + 2] = int(rank.replace("=", ""))
            return 2
        elif text_ls[position].count(" ") == 1:
            pos = text_ls[position].find('+')
            if pos != -1 and pos != 0 and text_ls[position][pos - 1] != " ":
                overall, behind = text_ls[position].split("+")
                behind2, rank = behind.split(" ")
                self.data.iat[j, column] = overall
                self.data.iat[j, column + 1] = "+" + behind2
                self.data.iat[j, column + 2] = int(rank.replace("=", ""))
                return 2
            else:
                overall, behind = text_ls[position].split(" ")
                self.data.iat[j, column] = overall
                self.data.iat[j, column + 1] = behind
                self.data.iat[j, column + 2] = int(text_ls[position + 1].replace('=', ""))
                return 1
        elif text_ls[position].count(" ") == 0:
            if text_ls[position].find('+') != -1 and text_ls[position].find('+') != 0:
                overall, behind = text_ls[position].split("+")
                self.data.iat[j, column] = overall
                self.data.iat[j, column + 1] = '+' + behind
                self.data.iat[j, column + 2] = text_ls[position + 1]
                return 1
            self.data.iat[j, column] = text_ls[position]
            if text_ls[position + 1].count(" ") == 1:
                behind, rank = text_ls[position + 1].split(" ")
                self.data.iat[j, column + 1] = behind
                self.data.iat[j, column + 2] = int(rank.replace('=', ""))
                return 1
            elif text_ls[position + 1].count("=") == 1:
                behind, rank = text_ls[position + 1].split("=")
                self.data.iat[j, column + 1] = behind
                self.data.iat[j, column + 2] = int(rank.replace('=', ""))
                return 1
            else:
                self.data.iat[j, column + 1] = text_ls[position + 1]
                print("bababa")
                if text_ls[position + 2].count(" ") == 1:
                    pattern = re.compile(r"\d{1,2}\s\d{1}")
                    if pattern.match(text_ls[position + 2]):
                        rank, misses = text_ls[position + 2].split(" ")
                        self.data.iat[j, column + 2] = int(rank.replace('=', ""))
                        self.data.iat[j, column + 3] = int(misses.replace('=', ""))
                        self.skip_flag = True
                    return 1
                else:
                    self.data.iat[j, column + 2] = int(text_ls[position + 2].replace('=', ""))
                    return 0

    def single_races(self, text_ls, index_list, minimum_limit, isindividual):
        """This method extracts specific race data from a biathlete of a PURSUIT (MASS START, INDIVIDUAL)
         race (e.g. interim times)"""

        for k, index_athlete in enumerate(index_list):
            if index_athlete >= minimum_limit:
                in_evaluation = index_list[:k]
                break
        else:
            in_evaluation = index_list

        add_indiv = 0
        if isindividual:
            add_indiv = 16
        j = 0
        for elem in in_evaluation:
            try:
                # name
                name = text_ls[elem]
                self.data.iat[j, 0] = name

                # country
                country = text_ls[elem + 1]
                self.data.iat[j, 1] = country

                # total misses
                # total_misses = text_ls[elem + 2]
                # self.data.iat[j, 2] = int(total_misses)

                # Only in individual races its probable that someone is more than 10 minutes
                # behind the winner. If this happens data cannot be read due to a different structure
                # than usual. This could be a solution but it's not clear if this backfires somewhere
                # else
                big_deficit_adjustment = 0

                # overall time
                if text_ls[elem + 2].count(" ") == 2:
                    misses, overall, behind = text_ls[elem + 2].split(" ")
                    self.data.iat[j, 2] = int(misses.replace("=", ""))
                    self.data.iat[j, 3] = overall
                    self.data.iat[j, 4] = behind
                    big_deficit_adjustment += 2
                elif text_ls[elem + 2].count(" ") == 1:
                    misses, overall = text_ls[elem + 2].split(" ")
                    pos = overall.find('+')
                    if pos != -1 and pos != 0 and overall[pos - 1] != " ":
                        overall, behind = overall.split("+")
                        big_deficit_adjustment += 1
                    self.data.iat[j, 2] = int(misses.replace("=", ""))
                    self.data.iat[j, 3] = overall
                    self.data.iat[j, 4] = behind
                    big_deficit_adjustment += 1
                elif text_ls[elem + 2].count(" ") == 0:
                    self.data.iat[j, 2] = int(text_ls[elem + 2].replace("=", ""))
                    if text_ls[elem + 3].count(" ") == 1:
                        overall, behind = text_ls[elem + 3].split(" ")
                        self.data.iat[j, 3] = overall
                        self.data.iat[j, 4] = behind
                        big_deficit_adjustment += 1
                    elif (text_ls[elem + 3].count(" ") == 0 and text_ls[elem + 3].find('+') != -1 and
                          text_ls[elem + 3].find('+') != 0 and
                          text_ls[elem + 3][text_ls[elem + 3].find('+') - 1] != " "):
                        overall, behind = text_ls[elem + 3].split("+")
                        self.data.iat[j, 3] = overall
                        self.data.iat[j, 4] = '+' + behind
                        big_deficit_adjustment += 1
                    else:
                        self.data.iat[j, 3] = text_ls[elem + 3]
                        self.data.iat[j, 4] = text_ls[elem + 4]

                total_rank = text_ls[elem + 5 - big_deficit_adjustment]
                self.data.iat[j, 5] = int(total_rank.replace('=', ""))

                # cumulative time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 7 - big_deficit_adjustment, 6, text_ls)

                # cumulative time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 10 - big_deficit_adjustment, 9, text_ls)

                # cumulative time loop 3
                big_deficit_adjustment += self.data_splitting(j, elem + 13 - big_deficit_adjustment, 12, text_ls)

                # cumulative time loop 4
                big_deficit_adjustment += self.data_splitting(j, elem + 16 - big_deficit_adjustment, 15, text_ls)

                # cumulative time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 19 - big_deficit_adjustment, 18, text_ls)

                # loop time 1
                big_deficit_adjustment += self.data_splitting(j, elem + 23 - big_deficit_adjustment, 21, text_ls)

                # loop time 2
                big_deficit_adjustment += self.data_splitting(j, elem + 26 - big_deficit_adjustment, 24, text_ls)

                # loop time 3
                big_deficit_adjustment += self.data_splitting(j, elem + 29 - big_deficit_adjustment, 27, text_ls)

                # loop time 4
                big_deficit_adjustment += self.data_splitting(j, elem + 32 - big_deficit_adjustment, 30, text_ls)

                # loop time 5
                big_deficit_adjustment += self.data_splitting(j, elem + 35 - big_deficit_adjustment, 33, text_ls)

                # individual race has a column 'ski times' which we want to skip but if there are values
                # with whitespaces in it we want to recognize that
                # if this is the case then increase big_deficit_adjustment
                if isindividual:
                    counter = 0
                    for i in range(39, 53):
                        pos = text_ls[elem + i - big_deficit_adjustment].find('+')
                        if pos != -1 and pos != 0 and text_ls[elem + i - big_deficit_adjustment][pos - 1] != " ":
                            counter += 1
                        if " " in text_ls[elem + i - big_deficit_adjustment]:
                            counter += text_ls[elem + i - big_deficit_adjustment].count(" ")
                    big_deficit_adjustment += counter

                # shooting misses loop 1
                for t in range(elem + 39 - big_deficit_adjustment + add_indiv - 15,
                               elem + 39 - big_deficit_adjustment + add_indiv + 15):
                    if text_ls[t] == 'Shooting':
                        while t != elem + 39 - big_deficit_adjustment + add_indiv:
                            if t > elem + 39 - big_deficit_adjustment + add_indiv:
                                big_deficit_adjustment -= 1
                            if t < elem + 39 - big_deficit_adjustment + add_indiv:
                                big_deficit_adjustment += 1
                        break
                shooting_misses_1 = text_ls[elem + 39 - big_deficit_adjustment + add_indiv]
                print('aaaa')
                if shooting_misses_1 == 'Shooting':
                    big_deficit_adjustment -= 1

                shooting_misses_1 = text_ls[elem + 39 - big_deficit_adjustment + add_indiv]
                self.data.iat[j, 36] = int(shooting_misses_1)

                # shooting time 1
                big_deficit_adjustment += self.data_splitting(j, elem + 40 + add_indiv - big_deficit_adjustment,
                                                              37, text_ls)

                # shooting misses loop 2
                if not self.skip_flag:
                    shooting_misses_2 = text_ls[elem + 43 - big_deficit_adjustment + add_indiv]
                    self.data.iat[j, 40] = int(shooting_misses_2)
                self.skip_flag = False

                # shooting loop time 2
                big_deficit_adjustment += self.data_splitting(j, elem + 44 + add_indiv - big_deficit_adjustment,
                                                              41, text_ls)

                # shooting misses loop 3
                if not self.skip_flag:
                    shooting_misses_3 = text_ls[elem + 47 - big_deficit_adjustment + add_indiv]
                    self.data.iat[j, 44] = int(shooting_misses_3)
                self.skip_flag = False

                # shooting loop time 3
                big_deficit_adjustment += self.data_splitting(j, elem + 48 + add_indiv - big_deficit_adjustment,
                                                              45, text_ls)
                # shooting misses loop 4
                if not self.skip_flag:
                    shooting_misses_4 = text_ls[elem + 51 - big_deficit_adjustment + add_indiv]
                    self.data.iat[j, 48] = int(shooting_misses_4)
                self.skip_flag = False

                # shooting loop time 4
                big_deficit_adjustment += self.data_splitting(j, elem + 52 + add_indiv - big_deficit_adjustment,
                                                              49, text_ls)

                # shooting misses overall
                if not self.skip_flag:
                    shooting_misses_overall = text_ls[elem + 55 - big_deficit_adjustment + add_indiv]
                    self.data.iat[j, 52] = int(shooting_misses_overall)
                self.skip_flag = False

                # shooting time overall
                big_deficit_adjustment += self.data_splitting(j, elem + 56 + add_indiv - big_deficit_adjustment,
                                                              53, text_ls)

                # range time loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 60 + add_indiv - big_deficit_adjustment,
                                                              56, text_ls)
                # range time loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 63 + add_indiv - big_deficit_adjustment,
                                                              59, text_ls)
                # range time loop 3
                big_deficit_adjustment += self.data_splitting(j, elem + 66 + add_indiv - big_deficit_adjustment,
                                                              62, text_ls)
                # range time loop 4
                big_deficit_adjustment += self.data_splitting(j, elem + 69 + add_indiv - big_deficit_adjustment,
                                                              65, text_ls)
                # range overall
                big_deficit_adjustment += self.data_splitting(j, elem + 72 + add_indiv - big_deficit_adjustment,
                                                              68, text_ls)
                # course loop 1
                big_deficit_adjustment += self.data_splitting(j, elem + 76 + add_indiv - big_deficit_adjustment,
                                                              71, text_ls)
                # course loop 2
                big_deficit_adjustment += self.data_splitting(j, elem + 79 + add_indiv - big_deficit_adjustment,
                                                              74, text_ls)
                # course loop 3
                big_deficit_adjustment += self.data_splitting(j, elem + 82 + add_indiv - big_deficit_adjustment,
                                                              77, text_ls)
                # course loop 4
                big_deficit_adjustment += self.data_splitting(j, elem + 85 + add_indiv - big_deficit_adjustment,
                                                              80, text_ls)
                # course loop 5
                big_deficit_adjustment += self.data_splitting(j, elem + 88 + add_indiv - big_deficit_adjustment,
                                                              83, text_ls)
                # course overall
                big_deficit_adjustment += self.data_splitting(j, elem + 91 + add_indiv - big_deficit_adjustment,
                                                              86, text_ls)
            except ValueError:
                print('Please insert the data for {} manual.'.format(text_ls[elem]))
                j += 1
                continue
            j += 1
        self.data = self.data.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)

    def get_data(self):
        """This method extracts specific race data from every biathlete (e.g. interim times, loop times)"""
        pages = cv.divide_into_pages(self.pdf_doc)
        text = ""
        for i in pages:
            text = text + i.getText('text')
        text_ls = text.split("\n")  # array of the lines of the string
        # update the athlete table in the database
        # index_list exist off integers which point to a name of an athlete in text_ls
        index_list = ba.update_athlete_db(text_ls)
        print(text_ls)
        # get the minimum limit, on which we try to stop reading data for the dataframe
        # if we would try to read after one of these words occur in the list we would get in danger to
        # provoke an IndexError because e.g. disqualified athletes might not have done a second lap
        # Therefore there is a whitespace in the pdf and no readable element in text_ls
        did_not_start_pos = did_not_finish_pos = lapped_pos = jury_dec_pos = disqualified_pos = len(text_ls)
        for pos, elem in enumerate(text_ls):
            if elem == 'Did not start':  # every keyword shouldn't occur more than once but BUGGGGG
                if did_not_start_pos == len(text_ls):
                    did_not_start_pos = pos
            if elem == 'Did not finish':
                if did_not_finish_pos == len(text_ls):
                    did_not_finish_pos = pos
            if elem == 'Lapped':
                if lapped_pos == len(text_ls):
                    lapped_pos = pos
            if elem == 'Jury Decisions':
                if jury_dec_pos == len(text_ls):
                    jury_dec_pos = pos
            if elem == 'Disqualified':
                if disqualified_pos == len(text_ls):
                    disqualified_pos = pos
        minimum_limit = min([did_not_finish_pos, did_not_start_pos, lapped_pos, jury_dec_pos, disqualified_pos])
        number_of_biathletes = 200

        columns_list = ['Name', 'Country', 'Total_Misses', 'Overall_Time', 'Overall_Time_Behind',
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

        self.data = pd.DataFrame(np.nan, index=list(range(number_of_biathletes)),
                                 columns=columns_list, dtype=object)

        if self.metadata['description'] == 'COMPETITION ANALYSIS':
            # if no race_type could be extracted stop this method get_data()
            if self.metadata['race_type'] is None:
                return
            elif self.metadata['race_type'] == 'SPRINT':
                self.sprint_data(text_ls, index_list, minimum_limit)
            elif self.metadata['race_type'] == 'PURSUIT':  # massenstart sollte gleich sein
                self.single_races(text_ls, index_list, minimum_limit, False)
            elif self.metadata['race_type'] == 'RELAY':
                pass
            elif self.metadata['race_type'] == 'INDIVIDUAL':
                self.single_races(text_ls, index_list, minimum_limit, True)
            elif self.metadata['race_type'] == 'MASS START':
                self.single_races(text_ls, index_list, minimum_limit, False)
            elif self.metadata['race_type'] == 'MIXED':
                pass

        types = ['string', 'string', int, 'string', 'string', int, 'string', 'string', int, 'string', 'string',
                 int, 'string', 'string', int, 'string', 'string', int, 'string', 'string', int, 'string',
                 'string', int, 'string', 'string', int, 'string', 'string', int, 'string', 'string',
                 int, 'string', 'string', int, int, 'string', 'string', int, int, 'string', 'string',
                 int, int, 'string', 'string', int, int, 'string', 'string', int, int, 'string', 'string',
                 int, 'string', 'string', int, 'string', 'string', int, 'string', 'string', int, 'string',
                 'string', int, 'string', 'string', int, 'string', 'string', int, 'string', 'string', int,
                 'string', 'string', int, 'string', 'string', int, 'string', 'string', int, 'string',
                 'string', int, 'string', 'string', 'string', 'string', 'string']
        for h, column in enumerate(columns_list):
            if self.data[column].isna().any().any():  # columns with null values are skipped
                continue
            self.data[column] = self.data[column].astype(types[h])

    def get_start_list(self):
        # index_list exist off integers which point to a name of an athlete in text_ls
        all_pages = cv.divide_into_pages(self.pdf_doc)
        all_text = ""
        for p in all_pages:
            all_text = all_text + p.getText('text')
        text_ls = all_text.split("\n")
        index_list = ba.update_athlete_db(text_ls)
        # if year is 2016 or bigger there is an adjustment because start lists have a different structure
        adjustment = 0
        if self.metadata['date'].year > 2016 or \
                (self.metadata['date'].year == 2016 and self.metadata['date'].month >= 6):
            adjustment = 1
        j = 0
        start_list = []
        if self.metadata['race_type'] is None:
            return
        elif self.metadata['race_type'] == 'SPRINT':
            for index_athlete in index_list:
                start_list.append((text_ls[index_athlete], text_ls[index_athlete + adjustment + 2]))
        elif self.metadata['race_type'] == 'PURSUIT':
            for index_athlete in index_list:
                start_list.append((text_ls[index_athlete].replace("-", " "), text_ls[index_athlete + 2]))
        elif self.metadata['race_type'] == 'INDIVIDUAL':
            for index_athlete in index_list:
                start_list.append((text_ls[index_athlete].replace("-", " "), text_ls[index_athlete + adjustment + 2]))
        elif self.metadata['race_type'] in ['RELAY', 'MASS START', 'MIXED']:
            for index_athlete in index_list:
                start_list.append((text_ls[index_athlete].replace("-", " "), self.metadata['race_start']))

        self.start_list = start_list

if __name__ == "__main__":
    # from string time to seconds: for comparisons
    # first split at milliseconds and add milliseconds later
    # secs = sum(int(x) * 60 ** i for i, x in enumerate(reversed(ts.split(':'))))

    organisation = 'WORLD CUP'
    pdf_doc_2_1 = fitz.Document(r"/DataProcessing/BT_C77D_1.0(5).pdf")
    pdf_doc_3 = fitz.Document("../Tests/BT_C82_1.0(1).pdf")
    pdf_doc_5 = fitz.Document("../Tests/BT_O77B_1.0(1).pdf")
    first = BiathlonData(pdf_doc_5, organisation)
    print(first.data.to_string())
