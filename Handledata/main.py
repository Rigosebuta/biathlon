"""This module invokes the other modules to transform all the data"""
import os

from Handledata import converting_data as cd, extracting_data as ed, biathlete as ba


def transform_data(path, organisation):
    """This method transform data from pdf to BiathlonData object -> 'main method'

        Args:
            path (str): path of the documents whose data will be extracted
            organisation (str): specify the organisation of the document
            -> the organisation cannot be extracted from the pdf and has to be specified

    """
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

    while organisation not in ['IBU CUP', 'WORLD CUP', 'OLYMPIC GAMES',
                               'WORLD CHAMPIONSHIPS', 'IBU JUNIOR CUP']:
        organisation = input('Please enter the organisation: (IBU CUP, WORLD CUP, OLYMPIC GAMES, '
                             'WORLD CHAMPIONSHIPS, IBU JUNIOR CUP): ')

    biathlon_data_ls = []
    for i in doc_ls:
        biathlon_data_ls.append(ed.BiathlonData(i, organisation))
    for doc in doc_ls:
        doc.close()
    return biathlon_data_ls


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


biathlon_data = transform_data(r'C:\Users\Michael\Documents\python_projects\biathlon\TestData', "WORLD CUP")
print(biathlon_data)
joined_biathlon = join_same_races(biathlon_data)
print(joined_biathlon)
print((len(biathlon_data)))
print(len(joined_biathlon))
print(len(biathlon_data) - len(joined_biathlon))
print()
# transform_data(r"C:\Users\Michael\Downloads")
# transform_data(r"E:\Biathlon 010203")


