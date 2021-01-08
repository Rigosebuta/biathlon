"""This module invokes the other modules to transform all the data"""
import os
from DataProcessing import converting_data as cd, extracting_data as ed, database_connection as dc


def transform_data(path, organisation):
    """This method transform data from pdf to BiathlonData object

        Args:
            path (str): path of the documents whose data will be extracted
            organisation (str): specify the organisation of the document
            -> the organisation cannot be extracted from the pdf and has to be specified

    """
    if not isinstance(path, str):
        raise TypeError
    if not isinstance(organisation, str):
        raise TypeError

    # filter by file extension 'pdf'
    documents = []
    for i in os.listdir(path):
        if i.split('.')[-1] == 'pdf':
            documents.append(path + "\\" + i)
    print(documents)
    doc_ls = cd.convert_pdf_to_document(documents)

    # valid_docs = cd.filter_doc(doc_ls)
    # print(valid_docs)

    invalid_docs = cd.is_not_pdf(doc_ls)
    if invalid_docs:
        print(invalid_docs, "This shouldn't happen. We filtered already by file extension. If there are"
                            "still existing files which are no pdfs, the convert_pdf_to_document method"
                            "(especially the PyMuPDF package) doesn't work properly")
        raise Exception  # !!!!!!!!!!!!!!!!!!!!!!!!!

    # if wrong 'organisation' input
    while organisation not in ['IBU CUP', 'WORLD CUP', 'OLYMPIC GAMES',
                               'WORLD CHAMPIONSHIPS', 'IBU JUNIOR CUP']:
        organisation = input('Please enter the organisation: (IBU CUP, WORLD CUP, OLYMPIC GAMES, '
                             'WORLD CHAMPIONSHIPS, IBU JUNIOR CUP): ')

    biathlon_data_ls = []
    for i in doc_ls:
        print(i)
        biathlon_data_ls.append(ed.BiathlonData(i, organisation))
    for doc in doc_ls:
        doc.close()
    return biathlon_data_ls


def data_to_database(biathlon_obj):

    place = biathlon_obj.metadata['place']
    date = biathlon_obj.metadata['date']
    race_type = biathlon_obj.metadata['race_type']
    age_group = biathlon_obj.metadata['age_group']
    gender = biathlon_obj.metadata['gender']
    race_id = (place, date, race_type, age_group, gender)
    if None in race_id:
        print('This should not happen. Please look into extracting_data -> get_metadata()')
        raise Exception

    conn = dc.get_connection()
    cursor = conn.cursor()
    get_rowid = "SELECT ROWID FROM Race WHERE place = ? AND date = ? AND type = ? AND age = ? AND gender = ? ;"
    cursor.execute(get_rowid, race_id)
    rows = cursor.fetchall()
    if rows:
        race_number = rows[0][0]
    else:
        key_row = "INSERT INTO Race (place, date, type, age, gender) " \
                      "VALUES(?, ?, ?, ?, ?);"
        cursor.execute(key_row, race_id)
        conn.commit()
        get_rowid = "SELECT ROWID FROM Race WHERE place = ? AND date = ? AND type = ? AND age = ? AND gender = ? ;"
        cursor.execute(get_rowid, race_id)
        rows = cursor.fetchall()
        race_number = rows[0][0]

    # insert additional data
    add_info = ['race_len_km', 'number_of_entries', 'did_not_start', 'did_not_finish', 'lapped',
                'disqualified', 'disqualified_for_unsportsmanlike_behaviour', 'ranked', 'weather',
                'snow_condition', 'snow_temperature', 'air_temperature', 'humidity', 'wind_direction',
                'wind_speed', 'total_course_length', 'height_difference', 'max_climb', 'total_climb',
                'level_difficulty']
    sql_none_values = []
    for i in add_info:
        if biathlon_obj.metadata[i] is None:
            continue
        sql_
    wenn wert schon besetzt, dann überprüfen ob es passt
    awenn nicht einfügen;)

        get_length = "SELECT length FROM RACE WHERE rowid = ?"
        cursor.execute(get_length, (race_number,))
        a = cursor.fetchall()

def main():
    """Data is accessible through https://biathlonresults.com. For getting usable data only use
    Start List, Competition Analysis and Competition Data Summary and only if they all exist"""

    dc.create_json_and_db()

    #biathlon_data = transform_data(r'C:\Users\Michael\Documents\python_projects\biathlon\Tests', "WORLD CUP")
    doc_ls = cd.convert_pdf_to_document([r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C77D_1.0(5).pdf'])
    a = ed.BiathlonData(doc_ls[0], 'WORLD CUP')
    data_to_database(a)
    #print(a.data)
    #print(a.start_list)
    #print(doc_ls)


    #joined_biathlon = join_same_races(biathlon_data)
    #print(joined_biathlon)
    #print((len(biathlon_data)))
    #print(len(joined_biathlon))
    #print(len(biathlon_data) - len(joined_biathlon))
    #print()
    #world_cup_2006_2007 = transform_data(r"C:\Users\Michael\Downloads")
    # transform_data(r"E:\Biathlon 010203")

    # data from external hard disk
    #transform_data(r"E:\Biathlon 010203", "WORLD CUP")...


if __name__ == "__main__":
    main()

