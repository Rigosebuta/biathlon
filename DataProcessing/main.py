"""This module invokes the other modules to transform the data"""
import os
from DataProcessing import converting_data as cd, extracting_data as ed, database_connection as dc


def transform_data(path, organisation):
    """This method transform data from a pdf to a BiathlonData object.

        :arg
            path (str): path of the documents whose data will be extracted
            organisation (str): specify the organisation of the races associated to the documents
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


def main():
    """Data is accessible through https://biathlonresults.com. For getting usable data only use
    Start List, Competition Analysis and Competition Data Summary and only if they all exist together"""

    dc.create_json_and_db()

    # biathlon_data = transform_data(r'C:\Users\Michael\Documents\python_projects\biathlon\Tests', "WORLD CUP")
    doc_ls = cd.convert_pdf_to_document(
        [r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C77D_1.0(5).pdf',
         r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C51C_1.0.pdf',
         r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C82_1.0.pdf'])
    a = []
    for d in doc_ls:
        a.append(ed.BiathlonData(d, 'WORLD CUP'))
    for am in a:
        dc.data_to_race_table(am)

    # world_cup_2006_2007 = transform_data(r"C:\Users\Michael\Downloads")
    # transform_data(r"E:\Biathlon 010203")

    # data from external hard disk
    # transform_data(r"E:\Biathlon 010203", "WORLD CUP")...


if __name__ == "__main__":
    main()
