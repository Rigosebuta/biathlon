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
                         'max_climb': None, 'total_climb': None, 'level_difficulty': None}
        self.get_metadata()

        self.data = None
        if self.metadata['description'] == "COMPETITION ANALYSIS":
            self.get_data()

        self.start_list = None
        if self.metadata['description'] == "START LIST":
            self.get_start_list()

    def get_metadata(self):
        """This method extracts metadata from the document."""

        # get first page of a pdf doc => first page is enough to find out some of the metadata
        page_zero = self.pdf_doc.loadPage(0)
        text_zero = page_zero.getText('text')

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

        print(self.metadata)

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
            print('This should not happen. Please check the method get_date')

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

    def sprint_data(self, text_ls, index_list, minimum_limit):
        """This method extracts specific race data from a biathlete of a SPRINT race (e.g. interim times)"""
        # divide into a index list of athletes who finished the race properly and those who didn't
        for k, index_athlete in enumerate(index_list):
            if index_athlete >= minimum_limit:
                in_evaluation = index_list[:k]
                out_of_evaluation = index_list[k:]
                break

        j = 0
        for elem in in_evaluation:
            # this try construct is enough because one it is in the wrong order there is following a
            # ValueError unless
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

                # overall time
                overall_time = text_ls[elem + 3]
                self.data.iat[j, 3] = overall_time

                # overall time behind
                overall_time_behind = text_ls[elem + 4]
                self.data.iat[j, 4] = overall_time_behind

                # overall rank
                overall_rank = text_ls[elem + 5].replace("=", "")
                self.data.iat[j, 5] = int(overall_rank)

                # cumulative time loop 1
                cumulative_time_loop_1 = text_ls[elem + 7]
                self.data.iat[j, 6] = cumulative_time_loop_1

                # cumulative time loop 1 behind
                cumulative_time_loop_1_behind = text_ls[elem + 8]
                self.data.iat[j, 7] = cumulative_time_loop_1_behind

                # cumulative time loop 1 rank
                cumulative_time_loop_1_rank = text_ls[elem + 9].replace("=", "")
                self.data.iat[j, 8] = int(cumulative_time_loop_1_rank)

                # cumulative time loop 2
                cumulative_time_loop_2 = text_ls[elem + 10]
                self.data.iat[j, 9] = cumulative_time_loop_2

                # cumulative time loop 2 behind
                cumulative_time_loop_2_behind = text_ls[elem + 11]
                self.data.iat[j, 10] = cumulative_time_loop_2_behind

                # cumulative time loop 2 rank
                cumulative_time_loop_2_rank = text_ls[elem + 12].replace("=", "")
                self.data.iat[j, 11] = int(cumulative_time_loop_2_rank)

                # cumulative time overall
                cumulative_time_overall = text_ls[elem + 13]
                self.data.iat[j, 18] = cumulative_time_overall

                # cumulative time overall behind
                cumulative_time_overall_behind = text_ls[elem + 14]
                self.data.iat[j, 19] = cumulative_time_overall_behind

                # cumulative time overall rank
                cumulative_time_overall_rank = text_ls[elem + 15].replace("=", "")
                self.data.iat[j, 20] = int(cumulative_time_overall_rank)

                # loop time 1
                loop_time_1 = text_ls[elem + 17]
                self.data.iat[j, 21] = loop_time_1

                # loop time 1 behind
                loop_time_1_behind = text_ls[elem + 18]
                self.data.iat[j, 22] = loop_time_1_behind

                # loop time 1 rank
                loop_time_1_rank = text_ls[elem + 19].replace("=", "")
                self.data.iat[j, 23] = int(loop_time_1_rank)

                # loop time 2
                loop_time_2 = text_ls[elem + 20]
                self.data.iat[j, 24] = loop_time_2

                # loop time 2 behind
                loop_time_2_behind = text_ls[elem + 21]
                self.data.iat[j, 25] = loop_time_2_behind

                # loop time 2 rank
                loop_time_2_rank = text_ls[elem + 22].replace("=", "")
                self.data.iat[j, 26] = int(loop_time_2_rank)

                # loop time 3
                loop_time_3 = text_ls[elem + 23]
                self.data.iat[j, 27] = loop_time_3

                # loop time 3 behind
                loop_time_3_behind = text_ls[elem + 24]
                self.data.iat[j, 28] = loop_time_3_behind

                # loop time 3 rank
                loop_time_3_rank = text_ls[elem + 25].replace("=", "")
                self.data.iat[j, 29] = int(loop_time_3_rank)

                # shooting misses loop 1
                shooting_misses_1 = text_ls[elem + 27]
                self.data.iat[j, 36] = int(shooting_misses_1)

                # shooting time 1
                shooting_time_1 = text_ls[elem + 28]
                self.data.iat[j, 37] = shooting_time_1

                # shooting time loop 1 behind
                shooting_time_loop_1_behind = text_ls[elem + 29]
                self.data.iat[j, 38] = shooting_time_loop_1_behind

                # shooting time loop 1 rank
                shooting_time_loop_1_rank = text_ls[elem + 30].replace("=", "")
                self.data.iat[j, 39] = int(shooting_time_loop_1_rank)

                # shooting misses loop 2
                shooting_misses_2 = text_ls[elem + 31]
                self.data.iat[j, 40] = int(shooting_misses_2)

                # shooting loop time 2
                shooting_loop_time_2 = text_ls[elem + 32]
                self.data.iat[j, 41] = shooting_loop_time_2

                # shooting loop time 2 behind
                shooting_loop_time_2_behind = text_ls[elem + 33]
                self.data.iat[j, 42] = shooting_loop_time_2_behind

                # shooting loop time 2 rank
                shooting_loop_time_2_rank = text_ls[elem + 34].replace("=", "")
                self.data.iat[j, 43] = int(shooting_loop_time_2_rank)

                # shooting misses overall
                shooting_misses_overall = text_ls[elem + 35]
                self.data.iat[j, 52] = int(shooting_misses_overall)

                # shooting time overall
                shooting_time_overall = text_ls[elem + 36]
                self.data.iat[j, 53] = shooting_time_overall

                # shooting time overall behind
                shooting_time_overall_behind = text_ls[elem + 37]
                self.data.iat[j, 54] = shooting_time_overall_behind

                # shooting time overall rank
                shooting_time_overall_rank = text_ls[elem + 38].replace("=", "")
                self.data.iat[j, 55] = int(shooting_time_overall_rank)

                # range time loop 1
                range_time_loop_1 = text_ls[elem + 40]
                self.data.iat[j, 56] = range_time_loop_1

                # range time loop 1 behind
                range_time_loop_1_behind = text_ls[elem + 41]
                self.data.iat[j, 57] = range_time_loop_1_behind

                # range time loop 1 rank
                range_time_loop_1_rank = text_ls[elem + 42].replace("=", "")
                self.data.iat[j, 58] = int(range_time_loop_1_rank)

                # range time loop 2
                range_time_loop_2 = text_ls[elem + 43]
                self.data.iat[j, 59] = range_time_loop_2

                # range time loop 2 behind
                range_time_loop_2_behind = text_ls[elem + 44]
                self.data.iat[j, 60] = range_time_loop_2_behind

                # range time loop 2 rank
                range_time_loop_2_rank = text_ls[elem + 45].replace("=", "")
                self.data.iat[j, 61] = int(range_time_loop_2_rank)

                # range time overall
                range_time_overall = text_ls[elem + 46]
                self.data.iat[j, 68] = range_time_overall

                # range time overall behind
                range_time_overall_behind = text_ls[elem + 47]
                self.data.iat[j, 69] = range_time_overall_behind

                # range time overall rank
                range_time_overall_rank = text_ls[elem + 48].replace("=", "")
                self.data.iat[j, 70] = int(range_time_overall_rank)

                # course time loop 1
                course_time_loop_1 = text_ls[elem + 50]
                self.data.iat[j, 71] = course_time_loop_1

                # course time loop 1 behind
                course_time_loop_1_behind = text_ls[elem + 51]
                self.data.iat[j, 72] = course_time_loop_1_behind

                # course time loop 1 rank
                course_time_loop_1_rank = text_ls[elem + 52].replace("=", "")
                self.data.iat[j, 73] = int(course_time_loop_1_rank)

                # course time loop 2
                course_time_loop_2 = text_ls[elem + 53]
                self.data.iat[j, 74] = course_time_loop_2

                # course time loop 2 behind
                course_time_loop_2_behind = text_ls[elem + 54]
                self.data.iat[j, 75] = course_time_loop_2_behind

                # course time loop 2 rank
                course_time_loop_2_rank = text_ls[elem + 55].replace("=", "")
                self.data.iat[j, 76] = int(course_time_loop_2_rank)

                # course time loop 3
                course_time_loop_3 = text_ls[elem + 56]
                self.data.iat[j, 77] = course_time_loop_3

                # course time loop 3 behind
                course_time_loop_3_behind = text_ls[elem + 57]
                self.data.iat[j, 78] = course_time_loop_3_behind

                # course time loop 3 rank
                course_time_loop_3_rank = text_ls[elem + 58].replace("=", "")
                self.data.iat[j, 79] = int(course_time_loop_3_rank)

                # course time overall
                course_time_overall = text_ls[elem + 59]
                self.data.iat[j, 86] = course_time_overall

                # course time overall behind
                course_time_overall_behind = text_ls[elem + 60]
                self.data.iat[j, 87] = course_time_overall_behind

                # course time overall rank
                course_time_overall_rank = text_ls[elem + 61].replace("=", "")
                self.data.iat[j, 88] = int(course_time_overall_rank)
                # PENALTY TIME = LOOP TIME - RANGE TIME - COURSE TIME
                # GESAMT PENALTY TIME = EINFACH ADDDIEREN
            except ValueError:
                print('Please insert the data for {} manual.'.format(text_ls[elem]))
                j += 1
                continue
            j += 1
        self.data = self.data.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)

    def pursuit_data(self, text_ls, index_list, minimum_limit):
        """This method extracts specific race data from a biathlete of a PURSUIT (MASS START)
         race (e.g. interim times)"""

        for k, index_athlete in enumerate(index_list):
            if index_athlete >= minimum_limit:
                in_evaluation = index_list[:k]
                break
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
                total_misses = text_ls[elem + 2]
                self.data.iat[j, 2] = int(total_misses)

                # overall time
                overall_time = text_ls[elem + 3]
                self.data.iat[j, 3] = overall_time

                # overall time behind
                overall_time_behind = text_ls[elem + 4]
                self.data.iat[j, 4] = overall_time_behind

                # overall rank
                overall_rank = text_ls[elem + 5].replace('=', "")
                self.data.iat[j, 5] = int(overall_rank)

                # cumulative time loop 1
                cumulative_time_loop_1 = text_ls[elem + 7]
                self.data.iat[j, 6] = cumulative_time_loop_1

                # cumulative time loop 1 behind
                cumulative_time_loop_1_behind = text_ls[elem + 8]
                self.data.iat[j, 7] = cumulative_time_loop_1_behind

                # cumulative time loop 1 rank
                cumulative_time_loop_1_rank = text_ls[elem + 9].replace('=', "")
                self.data.iat[j, 8] = int(cumulative_time_loop_1_rank)

                # cumulative time loop 2
                cumulative_time_loop_2 = text_ls[elem + 10]
                self.data.iat[j, 9] = cumulative_time_loop_2

                # cumulative time loop 2 behind
                cumulative_time_loop_2_behind = text_ls[elem + 11]
                self.data.iat[j, 10] = cumulative_time_loop_2_behind

                # cumulative time loop 2 rank
                cumulative_time_loop_2_rank = text_ls[elem + 12].replace('=', "")
                self.data.iat[j, 11] = int(cumulative_time_loop_2_rank)

                # cumulative time loop 3
                cumulative_time_loop_3 = text_ls[elem + 13]
                self.data.iat[j, 12] = cumulative_time_loop_3

                # cumulative time loop 3 behind
                cumulative_time_loop_3_behind = text_ls[elem + 14]
                self.data.iat[j, 13] = cumulative_time_loop_3_behind

                # cumulative time loop 3 rank
                cumulative_time_loop_3_rank = text_ls[elem + 15].replace('=', "")
                self.data.iat[j, 14] = int(cumulative_time_loop_3_rank)

                # cumulative time loop 4
                cumulative_time_loop_4 = text_ls[elem + 16]
                self.data.iat[j, 15] = cumulative_time_loop_4

                # cumulative time loop 4 behind
                cumulative_time_loop_4_behind = text_ls[elem + 17]
                self.data.iat[j, 16] = cumulative_time_loop_4_behind

                # cumulative time loop 4 rank
                cumulative_time_loop_4_rank = text_ls[elem + 18].replace('=', "")
                self.data.iat[j, 17] = int(cumulative_time_loop_4_rank)

                # cumulative time overall
                cumulative_time_overall = text_ls[elem + 19]
                self.data.iat[j, 18] = cumulative_time_overall

                # cumulative time overall behind
                cumulative_time_overall_behind = text_ls[elem + 20]
                self.data.iat[j, 19] = cumulative_time_overall_behind

                # cumulative time overall rank
                cumulative_time_overall_rank = text_ls[elem + 21].replace('=', "")
                self.data.iat[j, 20] = int(cumulative_time_overall_rank)

                # loop time 1
                loop_time_1 = text_ls[elem + 23]
                self.data.iat[j, 21] = loop_time_1

                # loop time 1 behind
                loop_time_1_behind = text_ls[elem + 24]
                self.data.iat[j, 22] = loop_time_1_behind

                # loop time 1 rank
                loop_time_1_rank = text_ls[elem + 25].replace('=', "")
                self.data.iat[j, 23] = int(loop_time_1_rank)

                # loop time 2
                loop_time_2 = text_ls[elem + 26]
                self.data.iat[j, 24] = loop_time_2

                # loop time 2 behind
                loop_time_2_behind = text_ls[elem + 27]
                self.data.iat[j, 25] = loop_time_2_behind

                # loop time 2 rank
                loop_time_2_rank = text_ls[elem + 28].replace('=', "")
                self.data.iat[j, 26] = int(loop_time_2_rank)

                # loop time 3
                loop_time_3 = text_ls[elem + 29]
                self.data.iat[j, 27] = loop_time_3

                # loop time 3 behind
                loop_time_3_behind = text_ls[elem + 30]
                self.data.iat[j, 28] = loop_time_3_behind

                # loop time 3 rank
                loop_time_3_rank = text_ls[elem + 31].replace('=', "")
                self.data.iat[j, 29] = int(loop_time_3_rank)

                # loop time 4
                loop_time_4 = text_ls[elem + 32]
                self.data.iat[j, 30] = loop_time_4

                # loop time 4 behind
                loop_time_4_behind = text_ls[elem + 33]
                self.data.iat[j, 31] = loop_time_4_behind

                # loop time 4 rank
                loop_time_4_rank = text_ls[elem + 34].replace('=', "")
                self.data.iat[j, 32] = int(loop_time_4_rank)

                # loop time 5
                loop_time_5 = text_ls[elem + 35]
                self.data.iat[j, 33] = loop_time_5

                # loop time 5 behind
                loop_time_5_behind = text_ls[elem + 36]
                self.data.iat[j, 34] = loop_time_5_behind

                # loop time 5 rank
                loop_time_5_rank = text_ls[elem + 37].replace('=', "")
                self.data.iat[j, 35] = int(loop_time_5_rank)

                # shooting misses loop 1
                shooting_misses_1 = text_ls[elem + 39]
                self.data.iat[j, 36] = int(shooting_misses_1)

                # shooting time 1
                shooting_time_1 = text_ls[elem + 40]
                self.data.iat[j, 37] = shooting_time_1

                # shooting time loop 1 behind
                shooting_time_loop_1_behind = text_ls[elem + 41]
                self.data.iat[j, 38] = shooting_time_loop_1_behind

                # shooting time loop 1 rank
                shooting_time_loop_1_rank = text_ls[elem + 42].replace('=', "")
                self.data.iat[j, 39] = int(shooting_time_loop_1_rank)

                # shooting misses loop 2
                shooting_misses_2 = text_ls[elem + 43]
                self.data.iat[j, 40] = int(shooting_misses_2)

                # shooting loop time 2
                shooting_loop_time_2 = text_ls[elem + 44]
                self.data.iat[j, 41] = shooting_loop_time_2

                # shooting loop time 2 behind
                shooting_loop_time_2_behind = text_ls[elem + 45]
                self.data.iat[j, 42] = shooting_loop_time_2_behind

                # shooting loop time 2 rank
                shooting_loop_time_2_rank = text_ls[elem + 46].replace('=', "")
                self.data.iat[j, 43] = int(shooting_loop_time_2_rank)

                # shooting misses loop 3
                shooting_misses_3 = text_ls[elem + 47]
                self.data.iat[j, 44] = int(shooting_misses_3)

                # shooting loop time 3
                shooting_loop_time_3 = text_ls[elem + 48]
                self.data.iat[j, 45] = shooting_loop_time_3

                # shooting loop time 3 behind
                shooting_loop_time_3_behind = text_ls[elem + 49]
                self.data.iat[j, 46] = shooting_loop_time_3_behind

                # shooting loop time 3 rank
                shooting_loop_time_3_rank = text_ls[elem + 50].replace('=', "")
                self.data.iat[j, 47] = int(shooting_loop_time_3_rank)

                # shooting misses loop 4
                shooting_misses_4 = text_ls[elem + 51]
                self.data.iat[j, 48] = int(shooting_misses_4)

                # shooting loop time 4
                shooting_loop_time_4 = text_ls[elem + 52]
                self.data.iat[j, 49] = shooting_loop_time_4

                # shooting loop time 4 behind
                shooting_loop_time_4_behind = text_ls[elem + 53]
                self.data.iat[j, 50] = shooting_loop_time_4_behind

                # shooting loop time 4 rank
                shooting_loop_time_4_rank = text_ls[elem + 54].replace('=', "")
                self.data.iat[j, 51] = int(shooting_loop_time_4_rank)

                # shooting misses overall
                shooting_misses_overall = text_ls[elem + 55]
                self.data.iat[j, 52] = int(shooting_misses_overall)

                # shooting time overall
                shooting_time_overall = text_ls[elem + 56]
                self.data.iat[j, 53] = shooting_time_overall

                # shooting time overall behind
                shooting_time_overall_behind = text_ls[elem + 57]
                self.data.iat[j, 54] = shooting_time_overall_behind

                # shooting time overall rank
                shooting_time_overall_rank = text_ls[elem + 58].replace('=', "")
                self.data.iat[j, 55] = int(shooting_time_overall_rank)

                # range time loop 1
                range_time_loop_1 = text_ls[elem + 60]
                self.data.iat[j, 56] = range_time_loop_1

                # range time loop 1 behind
                range_time_loop_1_behind = text_ls[elem + 61]
                self.data.iat[j, 57] = range_time_loop_1_behind

                # range time loop 1 rank
                range_time_loop_1_rank = text_ls[elem + 62].replace('=', "")
                self.data.iat[j, 58] = int(range_time_loop_1_rank)

                # range time loop 2
                range_time_loop_2 = text_ls[elem + 63]
                self.data.iat[j, 59] = range_time_loop_2

                # range time loop 2 behind
                range_time_loop_2_behind = text_ls[elem + 64]
                self.data.iat[j, 60] = range_time_loop_2_behind

                # range time loop 2 rank
                range_time_loop_2_rank = text_ls[elem + 65].replace('=', "")
                self.data.iat[j, 61] = int(range_time_loop_2_rank)

                # range time loop 3
                range_time_loop_3 = text_ls[elem + 66]
                self.data.iat[j, 62] = range_time_loop_3

                # range time loop 3 behind
                range_time_loop_3_behind = text_ls[elem + 67]
                self.data.iat[j, 63] = range_time_loop_3_behind

                # range time loop 3 rank
                range_time_loop_3_rank = text_ls[elem + 68].replace('=', "")
                self.data.iat[j, 64] = int(range_time_loop_3_rank)

                # range time loop 4
                range_time_loop_4 = text_ls[elem + 69]
                self.data.iat[j, 65] = range_time_loop_4

                # range time loop 4 behind
                range_time_loop_4_behind = text_ls[elem + 70]
                self.data.iat[j, 66] = range_time_loop_4_behind

                # range time loop 4 rank
                range_time_loop_4_rank = text_ls[elem + 71].replace('=', "")
                self.data.iat[j, 67] = int(range_time_loop_4_rank)

                # range time overall
                range_time_overall = text_ls[elem + 72]
                self.data.iat[j, 68] = range_time_overall

                # range time overall behind
                range_time_overall_behind = text_ls[elem + 73]
                self.data.iat[j, 69] = range_time_overall_behind

                # range time overall rank
                range_time_overall_rank = text_ls[elem + 74].replace('=', "")
                self.data.iat[j, 70] = int(range_time_overall_rank)

                # course time loop 1
                course_time_loop_1 = text_ls[elem + 76]
                self.data.iat[j, 71] = course_time_loop_1

                # course time loop 1 behind
                course_time_loop_1_behind = text_ls[elem + 77]
                self.data.iat[j, 72] = course_time_loop_1_behind

                # course time loop 1 rank
                course_time_loop_1_rank = text_ls[elem + 78].replace('=', "")
                self.data.iat[j, 73] = int(course_time_loop_1_rank)

                # course time loop 2
                course_time_loop_2 = text_ls[elem + 79]
                self.data.iat[j, 74] = course_time_loop_2

                # course time loop 2 behind
                course_time_loop_2_behind = text_ls[elem + 80]
                self.data.iat[j, 75] = course_time_loop_2_behind

                # course time loop 2 rank
                course_time_loop_2_rank = text_ls[elem + 81].replace('=', "")
                self.data.iat[j, 76] = int(course_time_loop_2_rank)

                # course time loop 3
                course_time_loop_3 = text_ls[elem + 82]
                self.data.iat[j, 77] = course_time_loop_3

                # course time loop 3 behind
                course_time_loop_3_behind = text_ls[elem + 83]
                self.data.iat[j, 78] = course_time_loop_3_behind

                # course time loop 3 rank
                course_time_loop_3_rank = text_ls[elem + 84].replace('=', "")
                self.data.iat[j, 79] = int(course_time_loop_3_rank)

                # course time loop 4
                course_time_loop_4 = text_ls[elem + 85]
                self.data.iat[j, 80] = course_time_loop_4

                # course time loop 4 behind
                course_time_loop_4_behind = text_ls[elem + 86]
                self.data.iat[j, 81] = course_time_loop_4_behind

                # course time loop 4 rank
                course_time_loop_4_rank = text_ls[elem + 87].replace('=', "")
                self.data.iat[j, 82] = int(course_time_loop_4_rank)

                # course time loop 5
                course_time_loop_5 = text_ls[elem + 88]
                self.data.iat[j, 83] = course_time_loop_5

                # course time loop 5 behind
                course_time_loop_5_behind = text_ls[elem + 89]
                self.data.iat[j, 84] = course_time_loop_5_behind

                # course time loop 5 rank
                course_time_loop_5_rank = text_ls[elem + 90].replace('=', "")
                self.data.iat[j, 85] = int(course_time_loop_5_rank)

                # course time overall
                course_time_overall = text_ls[elem + 91]
                self.data.iat[j, 86] = course_time_overall

                # course time overall behind
                course_time_overall_behind = text_ls[elem + 92]
                self.data.iat[j, 87] = course_time_overall_behind

                # course time overall rank
                course_time_overall_rank = text_ls[elem + 93].replace('=', "")
                self.data.iat[j, 88] = int(course_time_overall_rank)

                # PENALTY TIME = LOOP TIME - RANGE TIME - COURSE TIME
                # GESAMT PENALTY TIME = EINFACH ADDDIEREN
                # penalty_loop_1
                # penalty_loop_1 = text_ls[i + 94]
                # !!!
                # self.data.iat[j, 89] = penalty_loop_1

                # penalty_loop_2
                # penalty_loop_2 = text_ls[i + 95]
                # !!!
                # self.data.iat[j, 90] = penalty_loop_2

                # penalty_loop_3
                # penalty_loop_3 = text_ls[i + 96]
                # !!!
                # self.data.iat[j, 91] = penalty_loop_3

                # penalty_loop_4
                # penalty_loop_4 = text_ls[i + 97]
                # !!!
                # self.data.iat[j, 92] = penalty_loop_4

                # penalty_overall
                # penalty_loop_overall = text_ls[i + 98]
                # !!!
                # self.data.iat[j, 93] = penalty_loop_overall
            except ValueError:
                print('Please insert the data for {} manual.'.format(text_ls[elem]))
                j += 1
                continue
            j += 1
        self.data = self.data.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)

    def individual_data(self, text_ls, index_list, minimum_limit):
        """This method extracts specific race data from a biathlete of an INDIVIDUAL
        race (e.g. interim times)"""

        for k, index_athlete in enumerate(index_list):
            if index_athlete >= minimum_limit:
                in_evaluation = index_list[:k]
                break
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
                total_misses = text_ls[elem + 2]
                self.data.iat[j, 2] = int(total_misses)

                # Only in individual races its probable that someone is more than 10 minutes
                # behind the winner. If this happens data cannot be read due to a different structure
                # than usual. This could be a solution but it's not clear if this backfires somewhere
                # else
                big_deficit_adjustment = 0

                # overall time
                if " " in text_ls[elem + 3 - big_deficit_adjustment]:
                    overall_time, overall_time_behind = \
                        text_ls[elem + 3 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 3] = overall_time
                    self.data.iat[j, 4] = overall_time_behind
                    big_deficit_adjustment += 1
                else:
                    # overall time
                    overall_time = text_ls[elem + 3 - big_deficit_adjustment]
                    self.data.iat[j, 3] = overall_time

                    if " " in text_ls[elem + 4 - big_deficit_adjustment]:
                        overall_time_behind, overall_rank = \
                            text_ls[elem + 4 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 4] = overall_time_behind
                        self.data.iat[j, 5] = overall_rank.replace('=', "")
                        big_deficit_adjustment += 1
                    else:
                        # overall time behind
                        overall_time_behind = text_ls[elem + 4 - big_deficit_adjustment]
                        self.data.iat[j, 4] = overall_time_behind

                        # overall rank
                        overall_rank = text_ls[elem + 5 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 5] = int(overall_rank)

                # cumulative time loop 1
                if " " in text_ls[elem + 7 - big_deficit_adjustment]:
                    cumulative_time_loop_1, cumulative_time_loop_1_behind = \
                        text_ls[elem + 7 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 6] = cumulative_time_loop_1
                    self.data.iat[j, 7] = cumulative_time_loop_1_behind
                    big_deficit_adjustment += 1
                else:
                    # cumulative time loop 1
                    cumulative_time_loop_1 = text_ls[elem + 7 - big_deficit_adjustment]
                    self.data.iat[j, 6] = cumulative_time_loop_1

                    if " " in text_ls[elem + 8 - big_deficit_adjustment]:
                        cumulative_time_loop_1_behind, cumulative_time_loop_1_rank = \
                            text_ls[elem + 8 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 7] = cumulative_time_loop_1_behind
                        self.data.iat[j, 8] = int(cumulative_time_loop_1_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # cumulative time loop 1 behind
                        cumulative_time_loop_1_behind = text_ls[elem + 8 - big_deficit_adjustment]
                        self.data.iat[j, 7] = cumulative_time_loop_1_behind

                        # cumulative time loop 1 rank
                        cumulative_time_loop_1_rank = text_ls[elem + 9 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 8] = int(cumulative_time_loop_1_rank)

                # cumulative time loop 2
                if " " in text_ls[elem + 10 - big_deficit_adjustment]:
                    cumulative_time_loop_2, cumulative_time_loop_2_behind = \
                        text_ls[elem + 10 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 9] = cumulative_time_loop_2
                    self.data.iat[j, 10] = cumulative_time_loop_2_behind
                    big_deficit_adjustment += 1
                else:
                    # cumulative time loop 2
                    cumulative_time_loop_2 = text_ls[elem + 10 - big_deficit_adjustment]
                    self.data.iat[j, 9] = cumulative_time_loop_2

                    if " " in text_ls[elem + 11 - big_deficit_adjustment]:
                        cumulative_time_loop_2_behind, cumulative_time_loop_2_rank = \
                            text_ls[elem + 11 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 10] = cumulative_time_loop_2_behind
                        self.data.iat[j, 11] = int(cumulative_time_loop_2_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # cumulative time loop 2 behind
                        cumulative_time_loop_2_behind = text_ls[elem + 11 - big_deficit_adjustment]
                        self.data.iat[j, 10] = cumulative_time_loop_2_behind

                        # cumulative time loop 2 rank
                        cumulative_time_loop_2_rank = text_ls[elem + 12 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 11] = int(cumulative_time_loop_2_rank)

                # cumulative time loop 3
                if " " in text_ls[elem + 13 - big_deficit_adjustment]:
                    cumulative_time_loop_3, cumulative_time_loop_3_behind = \
                        text_ls[elem + 13 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 12] = cumulative_time_loop_3
                    self.data.iat[j, 13] = cumulative_time_loop_3_behind
                    big_deficit_adjustment += 1
                else:
                    # cumulative time loop 3
                    cumulative_time_loop_3 = text_ls[elem + 13 - big_deficit_adjustment]
                    self.data.iat[j, 12] = cumulative_time_loop_3

                    if " " in text_ls[elem + 14 - big_deficit_adjustment]:
                        cumulative_time_loop_3_behind, cumulative_time_loop_3_rank = \
                            text_ls[elem + 14 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 13] = cumulative_time_loop_3_behind
                        self.data.iat[j, 14] = int(cumulative_time_loop_3_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # cumulative time loop 3 behind
                        cumulative_time_loop_3_behind = text_ls[elem + 14 - big_deficit_adjustment]
                        self.data.iat[j, 13] = cumulative_time_loop_3_behind

                        # cumulative time loop 3 rank
                        cumulative_time_loop_3_rank = text_ls[elem + 15 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 14] = int(cumulative_time_loop_3_rank)

                # cumulative time loop 4
                if " " in text_ls[elem + 16 - big_deficit_adjustment]:
                    cumulative_time_loop_4, cumulative_time_loop_4_behind = \
                        text_ls[elem + 16 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 15] = cumulative_time_loop_4
                    self.data.iat[j, 16] = cumulative_time_loop_4_behind
                    big_deficit_adjustment += 1
                else:
                    # cumulative time loop 4
                    cumulative_time_loop_4 = text_ls[elem + 16 - big_deficit_adjustment]
                    self.data.iat[j, 15] = cumulative_time_loop_4

                    if " " in text_ls[elem + 17 - big_deficit_adjustment]:
                        cumulative_time_loop_4_behind, cumulative_time_loop_4_rank = \
                            text_ls[elem + 17 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 16] = cumulative_time_loop_4_behind
                        self.data.iat[j, 17] = int(cumulative_time_loop_4_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # cumulative time loop 4 behind
                        cumulative_time_loop_4_behind = text_ls[elem + 17 - big_deficit_adjustment]
                        self.data.iat[j, 16] = cumulative_time_loop_4_behind

                        # cumulative time loop 4 rank
                        cumulative_time_loop_4_rank = text_ls[elem + 18 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 17] = int(cumulative_time_loop_4_rank)

                # cumulative time overall
                if " " in text_ls[elem + 19 - big_deficit_adjustment]:
                    cumulative_time_overall, cumulative_time_overall_behind = \
                        text_ls[elem + 19 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 18] = cumulative_time_overall
                    self.data.iat[j, 19] = cumulative_time_overall_behind
                    big_deficit_adjustment += 1
                else:
                    # cumulative time overall
                    cumulative_time_overall = text_ls[elem + 19 - big_deficit_adjustment]
                    self.data.iat[j, 18] = cumulative_time_overall

                    if " " in text_ls[elem + 20 - big_deficit_adjustment]:
                        cumulative_time_overall_behind, cumulative_time_overall_rank = \
                            text_ls[elem + 20 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 19] = cumulative_time_overall_behind
                        self.data.iat[j, 20] = int(cumulative_time_overall_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # cumulative time overall behind
                        cumulative_time_overall_behind = text_ls[elem + 20 - big_deficit_adjustment]
                        self.data.iat[j, 19] = cumulative_time_overall_behind

                        # cumulative time overall rank
                        cumulative_time_overall_rank = text_ls[elem + 21 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 20] = int(cumulative_time_overall_rank)

                # loop time 1
                if " " in text_ls[elem + 23 - big_deficit_adjustment]:
                    loop_time_1, loop_time_1_behind = \
                        text_ls[elem + 23 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 21] = loop_time_1
                    self.data.iat[j, 22] = loop_time_1_behind
                    big_deficit_adjustment += 1
                else:
                    # loop time 1
                    loop_time_1 = text_ls[elem + 23 - big_deficit_adjustment]
                    self.data.iat[j, 21] = loop_time_1

                    if " " in text_ls[elem + 24 - big_deficit_adjustment]:
                        loop_time_1_behind, loop_time_1_rank = \
                            text_ls[elem + 24 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 22] = loop_time_1_behind
                        self.data.iat[j, 23] = int(loop_time_1_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # loop time 1 behind
                        loop_time_1_behind = text_ls[elem + 24 - big_deficit_adjustment]
                        self.data.iat[j, 22] = loop_time_1_behind

                        # loop time 1 rank
                        loop_time_1_rank = text_ls[elem + 25 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 23] = int(loop_time_1_rank)

                # loop time 2
                if " " in text_ls[elem + 26 - big_deficit_adjustment]:
                    loop_time_2, loop_time_2_behind = \
                        text_ls[elem + 26 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 24] = loop_time_2
                    self.data.iat[j, 25] = loop_time_2_behind
                    big_deficit_adjustment += 1
                else:
                    # loop time 2
                    loop_time_2 = text_ls[elem + 26 - big_deficit_adjustment]
                    self.data.iat[j, 24] = loop_time_2

                    if " " in text_ls[elem + 27 - big_deficit_adjustment]:
                        loop_time_2_behind, loop_time_2_rank = \
                            text_ls[elem + 27 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 25] = loop_time_2_behind
                        self.data.iat[j, 26] = int(loop_time_2_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # loop time 2 behind
                        loop_time_2_behind = text_ls[elem + 27 - big_deficit_adjustment]
                        self.data.iat[j, 25] = loop_time_2_behind

                        # loop time 2 rank
                        loop_time_2_rank = text_ls[elem + 28 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 26] = int(loop_time_2_rank)

                # loop time 3
                if " " in text_ls[elem + 29 - big_deficit_adjustment]:
                    loop_time_3, loop_time_3_behind = \
                        text_ls[elem + 29 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 27] = loop_time_3
                    self.data.iat[j, 28] = loop_time_3_behind
                    big_deficit_adjustment += 1
                else:
                    # loop time 3
                    loop_time_3 = text_ls[elem + 29 - big_deficit_adjustment]
                    self.data.iat[j, 27] = loop_time_3

                    if " " in text_ls[elem + 30 - big_deficit_adjustment]:
                        loop_time_3_behind, loop_time_3_rank = \
                            text_ls[elem + 30 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 28] = loop_time_3_behind
                        self.data.iat[j, 29] = int(loop_time_3_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # loop time 3 behind
                        loop_time_3_behind = text_ls[elem + 30 - big_deficit_adjustment]
                        self.data.iat[j, 28] = loop_time_3_behind

                        # loop time 3 rank
                        loop_time_3_rank = text_ls[elem + 31 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 29] = int(loop_time_3_rank)

                # loop time 4
                if " " in text_ls[elem + 32 - big_deficit_adjustment]:
                    loop_time_4, loop_time_4_behind = \
                        text_ls[elem + 32 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 30] = loop_time_4
                    self.data.iat[j, 31] = loop_time_4_behind
                    big_deficit_adjustment += 1
                else:
                    # loop time 4
                    loop_time_4 = text_ls[elem + 32 - big_deficit_adjustment]
                    self.data.iat[j, 30] = loop_time_4

                    if " " in text_ls[elem + 33 - big_deficit_adjustment]:
                        loop_time_4_behind, loop_time_4_rank = \
                            text_ls[elem + 33 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 31] = loop_time_4_behind
                        self.data.iat[j, 32] = int(loop_time_4_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # loop time 4 behind
                        loop_time_4_behind = text_ls[elem + 33 - big_deficit_adjustment]
                        self.data.iat[j, 31] = loop_time_4_behind

                        # loop time 4 rank
                        loop_time_4_rank = text_ls[elem + 34 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 32] = int(loop_time_4_rank)

                # loop time 5
                if " " in text_ls[elem + 35 - big_deficit_adjustment]:
                    loop_time_5, loop_time_5_behind = \
                        text_ls[elem + 35 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 33] = loop_time_5
                    self.data.iat[j, 34] = loop_time_5_behind
                    big_deficit_adjustment += 1
                else:
                    # loop time 5
                    loop_time_5 = text_ls[elem + 35 - big_deficit_adjustment]
                    self.data.iat[j, 33] = loop_time_5

                    if " " in text_ls[elem + 36 - big_deficit_adjustment]:
                        loop_time_5_behind, loop_time_5_rank = \
                            text_ls[elem + 36 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 34] = loop_time_5_behind
                        self.data.iat[j, 35] = int(loop_time_5_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # loop time 5 behind
                        loop_time_5_behind = text_ls[elem + 36 - big_deficit_adjustment]
                        self.data.iat[j, 34] = loop_time_5_behind

                        # loop time 5 rank
                        loop_time_5_rank = text_ls[elem + 37 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 35] = int(loop_time_5_rank)

                counter = 0
                for i in range(39, 54):
                    if " " in text_ls[elem + i - big_deficit_adjustment]:
                        counter += 1
                big_deficit_adjustment += counter

                # shooting misses loop 1
                shooting_misses_1 = text_ls[elem + 55 - big_deficit_adjustment]
                self.data.iat[j, 36] = int(shooting_misses_1)

                # shooting time 1
                shooting_time_1 = text_ls[elem + 56 - big_deficit_adjustment]
                self.data.iat[j, 37] = shooting_time_1

                # shooting 1
                if " " in text_ls[elem + 57 - big_deficit_adjustment]:
                    shooting_time_loop_1_behind, shooting_time_loop_1_rank = \
                        text_ls[elem + 57 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 38] = shooting_time_loop_1_behind
                    self.data.iat[j, 39] = int(shooting_time_loop_1_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # shooting time loop 1 behind
                    shooting_time_loop_1_behind = text_ls[elem + 57 - big_deficit_adjustment]
                    self.data.iat[j, 38] = shooting_time_loop_1_behind

                    # shooting time loop 1 rank
                    shooting_time_loop_1_rank = text_ls[elem + 58 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 39] = int(shooting_time_loop_1_rank)

                # shooting misses loop 2
                shooting_misses_2 = text_ls[elem + 59 - big_deficit_adjustment]
                self.data.iat[j, 40] = int(shooting_misses_2)

                # shooting loop time 2
                shooting_loop_time_2 = text_ls[elem + 60 - big_deficit_adjustment]
                self.data.iat[j, 41] = shooting_loop_time_2

                # shooting 2
                if " " in text_ls[elem + 61 - big_deficit_adjustment]:
                    shooting_time_loop_2_behind, shooting_time_loop_2_rank = \
                        text_ls[elem + 61 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 42] = shooting_time_loop_2_behind
                    self.data.iat[j, 43] = int(shooting_time_loop_2_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # shooting loop time 2 behind
                    shooting_loop_time_2_behind = text_ls[elem + 61 - big_deficit_adjustment]
                    self.data.iat[j, 42] = shooting_loop_time_2_behind

                    # shooting loop time 2 rank
                    shooting_loop_time_2_rank = text_ls[elem + 62 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 43] = int(shooting_loop_time_2_rank)

                # shooting misses loop 3
                shooting_misses_3 = text_ls[elem + 63 - big_deficit_adjustment]
                self.data.iat[j, 44] = int(shooting_misses_3)

                # shooting loop time 3
                shooting_loop_time_3 = text_ls[elem + 64 - big_deficit_adjustment]
                self.data.iat[j, 45] = shooting_loop_time_3

                # shooting 3
                if " " in text_ls[elem + 65 - big_deficit_adjustment]:
                    shooting_time_loop_3_behind, shooting_time_loop_3_rank = \
                        text_ls[elem + 65 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 46] = shooting_time_loop_3_behind
                    self.data.iat[j, 47] = int(shooting_time_loop_3_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # shooting loop time 3 behind
                    shooting_loop_time_3_behind = text_ls[elem + 65 - big_deficit_adjustment]
                    self.data.iat[j, 46] = shooting_loop_time_3_behind

                    # shooting loop time 3 rank
                    shooting_loop_time_3_rank = text_ls[elem + 66 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 47] = int(shooting_loop_time_3_rank)

                # shooting misses loop 4
                shooting_misses_4 = text_ls[elem + 67 - big_deficit_adjustment]
                self.data.iat[j, 48] = int(shooting_misses_4)

                # shooting loop time 4
                shooting_loop_time_4 = text_ls[elem + 68 - big_deficit_adjustment]
                self.data.iat[j, 49] = shooting_loop_time_4

                # shooting 4
                if " " in text_ls[elem + 69 - big_deficit_adjustment]:
                    shooting_time_loop_4_behind, shooting_time_loop_4_rank = \
                        text_ls[elem + 69 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 50] = shooting_time_loop_4_behind
                    self.data.iat[j, 51] = int(shooting_time_loop_4_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # shooting loop time 4 behind
                    shooting_loop_time_4_behind = text_ls[elem + 69 - big_deficit_adjustment]
                    self.data.iat[j, 50] = shooting_loop_time_4_behind

                    # shooting loop time 4 rank
                    shooting_loop_time_4_rank = text_ls[elem + 70 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 51] = int(shooting_loop_time_4_rank)

                # shooting misses overall
                shooting_misses_overall = text_ls[elem + 71 - big_deficit_adjustment]
                self.data.iat[j, 52] = int(shooting_misses_overall)

                # shooting time overall
                shooting_time_overall = text_ls[elem + 72 - big_deficit_adjustment]
                self.data.iat[j, 53] = shooting_time_overall

                # shooting overall
                if " " in text_ls[elem + 73 - big_deficit_adjustment]:
                    shooting_time_overall_behind, shooting_time_overall_rank = \
                        text_ls[elem + 73 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 54] = shooting_time_overall_behind
                    self.data.iat[j, 55] = int(shooting_time_overall_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # shooting time overall behind
                    shooting_time_overall_behind = text_ls[elem + 73 - big_deficit_adjustment]
                    self.data.iat[j, 54] = shooting_time_overall_behind

                    # shooting time overall rank
                    shooting_time_overall_rank = text_ls[elem + 74 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 55] = int(shooting_time_overall_rank)

                # range time loop 1
                range_time_loop_1 = text_ls[elem + 76 - big_deficit_adjustment]
                self.data.iat[j, 56] = range_time_loop_1

                # range loop 1
                if " " in text_ls[elem + 77 - big_deficit_adjustment]:
                    range_time_loop_1_behind, range_time_loop_1_rank = \
                        text_ls[elem + 77 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 57] = range_time_loop_1_behind
                    self.data.iat[j, 58] = int(range_time_loop_1_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # range time loop 1 behind
                    range_time_loop_1_behind = text_ls[elem + 77 - big_deficit_adjustment]
                    self.data.iat[j, 57] = range_time_loop_1_behind

                    # range time loop 1 rank
                    range_time_loop_1_rank = text_ls[elem + 78 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 58] = int(range_time_loop_1_rank)

                # range time loop 2
                range_time_loop_2 = text_ls[elem + 79 - big_deficit_adjustment]
                self.data.iat[j, 59] = range_time_loop_2

                # range loop 2
                if " " in text_ls[elem + 80 - big_deficit_adjustment]:
                    range_time_loop_2_behind, range_time_loop_2_rank = \
                        text_ls[elem + 80 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 60] = range_time_loop_2_behind
                    self.data.iat[j, 61] = int(range_time_loop_2_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # range time loop 2 behind
                    range_time_loop_2_behind = text_ls[elem + 80 - big_deficit_adjustment]
                    self.data.iat[j, 60] = range_time_loop_2_behind

                    # range time loop 2 rank
                    range_time_loop_2_rank = text_ls[elem + 81 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 61] = int(range_time_loop_2_rank)

                # range time loop 3
                range_time_loop_3 = text_ls[elem + 82 - big_deficit_adjustment]
                self.data.iat[j, 62] = range_time_loop_3

                # range loop 3
                if " " in text_ls[elem + 83 - big_deficit_adjustment]:
                    range_time_loop_3_behind, range_time_loop_3_rank = \
                        text_ls[elem + 77 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 63] = range_time_loop_3_behind
                    self.data.iat[j, 64] = int(range_time_loop_3_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # range time loop 3 behind
                    range_time_loop_3_behind = text_ls[elem + 83 - big_deficit_adjustment]
                    self.data.iat[j, 63] = range_time_loop_3_behind

                    # range time loop 3 rank
                    range_time_loop_3_rank = text_ls[elem + 84 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 64] = int(range_time_loop_3_rank)

                # range time loop 4
                range_time_loop_4 = text_ls[elem + 85 - big_deficit_adjustment]
                self.data.iat[j, 65] = range_time_loop_4

                # range loop 4
                if " " in text_ls[elem + 86 - big_deficit_adjustment]:
                    range_time_loop_4_behind, range_time_loop_4_rank = \
                        text_ls[elem + 86 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 66] = range_time_loop_4_behind
                    self.data.iat[j, 67] = int(range_time_loop_4_rank.replace('=', ""))
                    big_deficit_adjustment += 1
                else:
                    # range time loop 4 behind
                    range_time_loop_4_behind = text_ls[elem + 86 - big_deficit_adjustment]
                    self.data.iat[j, 66] = range_time_loop_4_behind

                    # range time loop 4 rank
                    range_time_loop_4_rank = text_ls[elem + 87 - big_deficit_adjustment].replace('=', "")
                    self.data.iat[j, 67] = int(range_time_loop_4_rank)

                # range overall
                if " " in text_ls[elem + 88 - big_deficit_adjustment]:
                    range_time_overall, range_time_overall_behind = \
                        text_ls[elem + 88 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 68] = range_time_overall
                    self.data.iat[j, 69] = range_time_overall_behind
                    big_deficit_adjustment += 1
                else:
                    # range time overall
                    range_time_overall = text_ls[elem + 88 - big_deficit_adjustment]
                    self.data.iat[j, 68] = range_time_overall

                    # range overall
                    if " " in text_ls[elem + 89 - big_deficit_adjustment]:
                        range_time_overall_behind, range_time_overall_rank = \
                            text_ls[elem + 89 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 69] = range_time_overall_behind
                        self.data.iat[j, 70] = int(range_time_overall_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # range time overall behind
                        range_time_overall_behind = text_ls[elem + 89 - big_deficit_adjustment]
                        self.data.iat[j, 69] = range_time_overall_behind

                        # range time overall rank
                        range_time_overall_rank = text_ls[elem + 90 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 70] = int(range_time_overall_rank)

                # course loop 1
                if " " in text_ls[elem + 92 - big_deficit_adjustment]:
                    course_time_loop_1, course_time_loop_1_behind = \
                        text_ls[elem + 92 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 71] = course_time_loop_1
                    self.data.iat[j, 72] = course_time_loop_1_behind
                    big_deficit_adjustment += 1
                else:
                    # course time loop 1
                    course_time_loop_1 = text_ls[elem + 92 - big_deficit_adjustment]
                    self.data.iat[j, 71] = course_time_loop_1

                    if " " in text_ls[elem + 93 - big_deficit_adjustment]:
                        course_time_loop_1_behind, course_time_loop_1_rank = \
                            text_ls[elem + 93 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 72] = course_time_loop_1_behind
                        self.data.iat[j, 73] = int(course_time_loop_1_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time loop 1 behind
                        course_time_loop_1_behind = text_ls[elem + 93 - big_deficit_adjustment]
                        self.data.iat[j, 72] = course_time_loop_1_behind

                        # course time loop 1 rank
                        course_time_loop_1_rank = text_ls[elem + 94 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 73] = int(course_time_loop_1_rank)

                # course loop 2
                if " " in text_ls[elem + 95 - big_deficit_adjustment]:
                    course_time_loop_2, course_time_loop_2_behind = \
                        text_ls[elem + 95 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 74] = course_time_loop_2
                    self.data.iat[j, 75] = course_time_loop_2_behind
                    big_deficit_adjustment += 1
                else:
                    # course time loop 2
                    course_time_loop_2 = text_ls[elem + 95 - big_deficit_adjustment]
                    self.data.iat[j, 74] = course_time_loop_2

                    if " " in text_ls[elem + 96 - big_deficit_adjustment]:
                        course_time_loop_2_behind, course_time_loop_2_rank = \
                            text_ls[elem + 96 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 75] = course_time_loop_2_behind
                        self.data.iat[j, 76] = int(course_time_loop_2_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time loop 2 behind
                        course_time_loop_2_behind = text_ls[elem + 96 - big_deficit_adjustment]
                        self.data.iat[j, 75] = course_time_loop_2_behind

                        # course time loop 2 rank
                        course_time_loop_2_rank = text_ls[elem + 97 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 76] = int(course_time_loop_2_rank)

                # course loop 3
                if " " in text_ls[elem + 98 - big_deficit_adjustment]:
                    course_time_loop_3, course_time_loop_3_behind = \
                        text_ls[elem + 98 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 77] = course_time_loop_3
                    self.data.iat[j, 78] = course_time_loop_3_behind
                    big_deficit_adjustment += 1
                else:
                    # course time loop 3
                    course_time_loop_3 = text_ls[elem + 98 - big_deficit_adjustment]
                    self.data.iat[j, 77] = course_time_loop_3

                    if " " in text_ls[elem + 99 - big_deficit_adjustment]:
                        course_time_loop_3_behind, course_time_loop_3_rank = \
                            text_ls[elem + 99 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 78] = course_time_loop_3_behind
                        self.data.iat[j, 79] = int(course_time_loop_3_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time loop 3 behind
                        course_time_loop_3_behind = text_ls[elem + 99 - big_deficit_adjustment]
                        self.data.iat[j, 78] = course_time_loop_3_behind

                        # course time loop 3 rank
                        course_time_loop_3_rank = text_ls[elem + 100 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 79] = int(course_time_loop_3_rank)

                # course loop 4
                if " " in text_ls[elem + 101 - big_deficit_adjustment]:
                    course_time_loop_4, course_time_loop_4_behind = \
                        text_ls[elem + 101 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 80] = course_time_loop_4
                    self.data.iat[j, 81] = course_time_loop_4_behind
                    big_deficit_adjustment += 1
                else:
                    # course time loop 4
                    course_time_loop_4 = text_ls[elem + 101 - big_deficit_adjustment]
                    self.data.iat[j, 80] = course_time_loop_4

                    if " " in text_ls[elem + 102 - big_deficit_adjustment]:
                        course_time_loop_4_behind, course_time_loop_4_rank = \
                            text_ls[elem + 102 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 81] = course_time_loop_4_behind
                        self.data.iat[j, 82] = int(course_time_loop_4_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time loop 4 behind
                        course_time_loop_4_behind = text_ls[elem + 102 - big_deficit_adjustment]
                        self.data.iat[j, 81] = course_time_loop_4_behind

                        # course time loop 4 rank
                        course_time_loop_4_rank = text_ls[elem + 103 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 82] = int(course_time_loop_4_rank)

                # course loop 5
                if " " in text_ls[elem + 104 - big_deficit_adjustment]:
                    course_time_loop_5, course_time_loop_5_behind = \
                        text_ls[elem + 104 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 83] = course_time_loop_5
                    self.data.iat[j, 84] = course_time_loop_5_behind
                    big_deficit_adjustment += 1
                else:
                    # course time loop 5
                    course_time_loop_5 = text_ls[elem + 104 - big_deficit_adjustment]
                    self.data.iat[j, 83] = course_time_loop_5

                    if " " in text_ls[elem + 105 - big_deficit_adjustment]:
                        course_time_loop_5_behind, course_time_loop_5_rank = \
                            text_ls[elem + 105 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 84] = course_time_loop_5_behind
                        self.data.iat[j, 85] = int(course_time_loop_5_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time loop 5 behind
                        course_time_loop_5_behind = text_ls[elem + 105 - big_deficit_adjustment]
                        self.data.iat[j, 84] = course_time_loop_5_behind

                        # course time loop 5 rank
                        course_time_loop_5_rank = text_ls[elem + 106 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 85] = int(course_time_loop_5_rank)

                if " " in text_ls[elem + 107 - big_deficit_adjustment]:
                    course_time_overall, course_time_overall_behind = \
                        text_ls[elem + 107 - big_deficit_adjustment].split(" ")
                    self.data.iat[j, 86] = course_time_overall
                    self.data.iat[j, 87] = course_time_overall_behind
                    big_deficit_adjustment += 1
                else:
                    # course time overall
                    course_time_overall = text_ls[elem + 107 - big_deficit_adjustment]
                    self.data.iat[j, 86] = course_time_overall

                    if " " in text_ls[elem + 108 - big_deficit_adjustment]:
                        course_time_overall_behind, course_time_overall_rank = \
                            text_ls[elem + 108 - big_deficit_adjustment].split(" ")
                        self.data.iat[j, 87] = course_time_overall_behind
                        self.data.iat[j, 88] = int(course_time_overall_rank.replace('=', ""))
                        big_deficit_adjustment += 1
                    else:
                        # course time overall behind
                        course_time_overall_behind = text_ls[elem + 108 - big_deficit_adjustment]
                        self.data.iat[j, 87] = course_time_overall_behind

                        # course time overall rank
                        course_time_overall_rank = text_ls[elem + 109 - big_deficit_adjustment].replace('=', "")
                        self.data.iat[j, 88] = int(course_time_overall_rank)

                # PENALTY TIME = LOOP TIME - RANGE TIME - COURSE TIME
                # GESAMT PENALTY TIME = EINFACH ADDDIEREN
                # penalty_loop_1
                # penalty_loop_1 = text_ls[i + 94]
                # !!!
                # self.data.iat[j, 89] = penalty_loop_1

                # penalty_loop_2
                # penalty_loop_2 = text_ls[i + 95]
                # !!!
                # self.data.iat[j, 90] = penalty_loop_2

                # penalty_loop_3
                # penalty_loop_3 = text_ls[i + 96]
                # !!!
                # self.data.iat[j, 91] = penalty_loop_3

                # penalty_loop_4
                # penalty_loop_4 = text_ls[i + 97]
                # !!!
                # self.data.iat[j, 92] = penalty_loop_4

                # penalty_overall
                # penalty_loop_overall = text_ls[i + 98]
                # !!!
                # self.data.iat[j, 93] = penalty_loop_overall
            except ValueError:
                print('Data error => Please insert the data for {} manual.'.format(text_ls[elem]))
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
        # print(text_ls)
        # update the athlete table in the database
        index_list = ba.update_athlete_db(text_ls)

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

        if self.metadata['description'] == 'COMPETITION ANALYSIS':
            # if no race_type could be extracted stop this method get_data()
            if self.metadata['race_type'] is None:
                return
            elif self.metadata['race_type'] == 'SPRINT':
                self.sprint_data(text_ls, index_list, minimum_limit)
            elif self.metadata['race_type'] == 'PURSUIT':  # massenstart sollte gleich sein
                self.pursuit_data(text_ls, index_list, minimum_limit)
            elif self.metadata['race_type'] == 'RELAY':
                pass
            elif self.metadata['race_type'] == 'INDIVIDUAL':
                self.individual_data(text_ls, index_list, minimum_limit)
            elif self.metadata['race_type'] == 'MASS START':
                self.pursuit_data(text_ls, index_list, minimum_limit)
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
        self.start_list = 0
        all_pages = cv.divide_into_pages(self.pdf_doc)
        all_text = ""
        for p in all_pages:
            all_text = all_text + p.getText('text')
        all_text_ls = all_text.split("\n")
        print(all_text_ls)
        self.start_list = all_text_ls


if __name__ == "__main__":
    # from string time to seconds: for comparisons
    # first split at milliseconds and add milliseconds later
    # secs = sum(int(x) * 60 ** i for i, x in enumerate(reversed(ts.split(':'))))

    organisation = 'WORLD CUP'
    # pdf_doc_1 = fitz.Document("../Tests/BT_C51A_1.0(1).pdf")
    pdf_doc_2_1 = fitz.Document(r"C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C77D_1.0(5).pdf")
    pdf_doc_3 = fitz.Document("../Tests/BT_C82_1.0(1).pdf")
    # pdf_doc_4 = fitz.Document("../Tests/BT_O77B_1.0.pdf")
    pdf_doc_5 = fitz.Document("../Tests/BT_O77B_1.0(1).pdf")
    # pdf_doc_2_2 = fitz.Document("../Tests/BT_C82_1.0.pdf")
    # pdf_doc_2_3 = fitz.Document("../Tests/BT_C51C_1.0.pdf")

    # pages = cv.divide_into_pages(pdf_doc_6)
    # text = ""
    # for i in pages:
    #    text = text + i.getText('text')
    # text_ls = text.split("\n")  # array of the lines of the string
    # print(text_ls)
    first = BiathlonData(pdf_doc_5, organisation)
    # second_1 = BiathlonData(pdf_doc_2_1, organisation)
    # second_2 = BiathlonData(pdf_doc_2_2, organisation)
    # second_3 = BiathlonData(pdf_doc_2_3, organisation)
    # print(first.metadata)
    print(first.data.to_string())

    # print(second_3.start_list)
    # print(second_1.data.to_string())
    # print(second_1.metadata)
    # print(second_2.metadata)
    # print(second_3.metadata)

    # print(second.metadata)
    # print(second.data.head())
    # third = BiathlonData(pdf_doc_3, organisation)
