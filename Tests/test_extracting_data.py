import pytest
from DataProcessing import extracting_data as ed, converting_data as cd

@pytest.fixture
def data():
    doc_ls = cd.convert_pdf_to_document(
        [r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C77D_1.0(5).pdf',
         r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C51C_1.0.pdf',
         r'C:\Users\Michael\Documents\python_projects\biathlon\Tests\BT_C82_1.0.pdf'])
    return doc_ls


# This method also shows that no exception is raised from the methods get_metadata(),
# get_data() and get_start_lists()
def test_biathlon_data_constructor(data, monkeypatch):
    """This method tests if the constructor of BiathlonData works properly."""
    biathlon_data_ls = []
    for d in data:
        # we know that only the place has to be entered from the user into the metadata
        monkeypatch.setattr('builtins.input', lambda _: "FORT KENT")
        biathlon_data_ls.append(ed.BiathlonData(d, 'WORLD CUP'))
    for elem in biathlon_data_ls:
        assert elem.organisation == 'WORLD CUP'
        # this would race an exception if variables are not existing
        a, b, c, d = elem.pdf_doc, elem.data, elem.metadata, elem.start_list


# instead of testing all the methods we pick with some random values to test if these are right.

