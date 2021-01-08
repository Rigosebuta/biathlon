"""This module invokes the other modules to transform all the data"""
import os
from DataProcessing import converting_data as cd, extracting_data as ed, database_connection as dc
import pandas as pd


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

    not_none_list = [biathlon_obj.metadata['place'], biathlon_obj.metadata['date'],
                     biathlon_obj.metadata['race_type'], biathlon_obj.metadata['age_group'],
                     biathlon_obj.metadata['gender']]
    if None in not_none_list:
        print('This should not happen. Please look into extractinng_data -> get_metadata()')
        raise Exception

    conn = dc.get_connection()
    athlete_table = pd.read_sql_query(
        '''SELECT *
        FROM Race
        WHERE place = biathlon_obj.metadata AND date = biathlon_obj.metadata''', conn)

    df = pd.DataFrame(athlete_table, columns=['place', 'date', 'type', 'age', 'gender'])
    if empty dann ein neues kreiren
        ansonsten row_id zurückgeben
    print(df.to_string())

    sql_statement = """
                SELECT *
                FROM Race
                WHERE """
    for index, row in df.iterrows():
        print(index, row['place'], row['age'])
        if
def join_same_races(biathlon_data_ls):  # works only if everything else in extracting_data works
    """This method joins two BiathlonData objects"""
    ls = []

    for i, j in enumerate(biathlon_data_ls):
        place = j.metadata['place']
        date = j.metadata['date']
        race_typ = j.metadata['race_type']
        gender = j.metadata['gender']
        if i + 1 < len(biathlon_data_ls):
            for k in biathlon_data_ls[i + 1:]:
                if place == k.metadata['place'] and \
                        date == k.metadata['date'] and \
                        race_typ == k.metadata['race_type'] and \
                        gender == k.metadata['gender']:
                    for key in j.metadata:
                        if j.metadata[key] is None:
                            j.metadata[key] = k.metadata[key]
                        elif j.metadata[key] is not None and \
                                j.metadata[key] is not None and \
                                (not j.metadata[key] == j.metadata[key]):
                            print("Please look into join_same_races()")
                            raise Exception
                    if j.data is None:
                        j.data = k.data
                    if j.start_list is None:
                        j.start_list = k.start_list
                    ls.append(j)
    return [complete for complete in ls if complete.start_list
            is not None and complete.data is not None and
            complete.metadata['total_course_length'] is not None]


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
