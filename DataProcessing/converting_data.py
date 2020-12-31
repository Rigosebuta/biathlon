"""This module convert files into 'Document' objects

    - For more information about the PyMuPDF package and
    therefore fitz: https://pymupdf.readthedocs.io/en/latest/
    - For more information about the 'Document' class
    from PyMuPDF: https://pymupdf.readthedocs.io/en/latest/document.html
"""


import re
import fitz


def convert_pdf_to_document(file_list):
    """This method converts a list of strings (filenames) into a list of 'Document' objects

        Args:
            file_list (list): list of file names as strings
        Returns:
            list of 'Document' objects associated to the file names
    """
    if not isinstance(file_list, list):
        raise TypeError

    doc_ls = []

    for filename in file_list:
        if not isinstance(filename, str):
            raise TypeError
        if not filename[-3:] == 'pdf':
            raise FileExistsError
        doc_ls.append(fitz.Document(filename))
    return doc_ls


def filter_doc(doc_ls):
    """This method returns a list of documents which are pdfs

        Args:
            doc_ls (list): a list of 'Document' objects
        Returns:
            the input list with the condition that every object in the list is a pdf
            -> a document, which is not a pdf, will be rejected
    """
    if not isinstance(doc_ls, list):
        raise TypeError
    for i in doc_ls:
        if not isinstance(i, fitz.Document):
            raise TypeError
    return [doc for doc in doc_ls if doc.isPDF]


def is_not_pdf(doc_ls):
    """This method returns a list of documents which are not pdfs

        Args:
            doc_ls (list): a list of 'Document' objects
        Returns:
            the input list with the condition that every object in the list is not a pdf
            -> a document, which is a pdf, will be rejected
    """
    if not isinstance(doc_ls, list):
        raise TypeError
    for i in doc_ls:
        if not isinstance(i, fitz.Document):
            raise TypeError
    return [doc for doc in doc_ls if (not doc.isPDF)]


def divide_into_pages(pdf_doc):
    """This method divides every document(pdf) into pages

        Args:
            pdf_doc (Document): a 'Document' object
        Returns:
            list of pages of the input document

        - For more information about the page class: https://pymupdf.readthedocs.io/en/latest/page.html
    """
    if not isinstance(pdf_doc, fitz.Document):
        raise TypeError
    if not pdf_doc.isPDF:
        raise TypeError

    pages = []
    for i in range(pdf_doc.pageCount):
        page = pdf_doc.loadPage(i)
        pages.append(page)
    return pages


def from_one_unity_to_float(unity_ls, sep):
    """This method transforms a string (degree in celsius) list into a float list

        Args:
            unity_ls (str list): list with strings with celsius-in-degree values
            sep: separator for the string split
    """
    float_ls = []
    if not isinstance(unity_ls, list):
        raise TypeError
    if not isinstance(sep, str):
        raise TypeError
    for elem in unity_ls:
        if not isinstance(elem, str):
            raise TypeError
        if sep not in elem:
            raise TypeError
        n_ls = elem.split(sep)
        temp = re.compile(r'-?[0-9]{1,2}[.,][0-9]{1,2}')
        matches = [elem for elem in n_ls if temp.match(elem)]
        if not len(matches) == 1:
            print('This should not happen', "Please look into from_one_unity_to_float")
            raise TypeError
        else:
            matches_str = "".join(matches)  # convert list to string
            matches_str = matches_str.replace(" ", "")
            matches_str = matches_str.replace(",", ".")
            matches_float = float(matches_str)
        if matches:
            float_ls.append(matches_float)
        else:
            print("This should not happen. Please look into from_one_unity_to_float")
            raise TypeError
    return float_ls


def eliminating_leading_zero(number_as_string):
    """This method eliminates leading zeros of a string of max. length 2"""
    if not isinstance(number_as_string, str):
        raise TypeError
    if len(number_as_string) == 0:
        raise TypeError
    if number_as_string[0] == '0':
        return number_as_string[1:]
    return number_as_string


def get_time(str_time):
    """This method converts a string appearing in the data into a "time" object"""

    time_ls = str_time.split(":")
    if len(time_ls) > 2:
        try:
            time_ls = [eliminating_leading_zero(time) for time in time_ls]
            hours = int(time_ls[0])
            minutes = int(time_ls[1])
            seconds_ls = time_ls[2].split(".")
            seconds_ls = [eliminating_leading_zero(sec) for sec in seconds_ls]
            seconds = int(seconds_ls[0])
            milliseconds = int(seconds_ls[1])
        except ValueError:
            print("This should not happen. Please look into get_time()")
    else:
        try:
            hours = 0
            time_ls = [eliminating_leading_zero(time) for time in time_ls]
            minutes = int(time_ls[0])
            seconds_ls = time_ls[1].split(".")
            seconds_ls = [eliminating_leading_zero(sec) for sec in seconds_ls]
            seconds = int(seconds_ls[0])
            milliseconds = int(seconds_ls[1])

        except ValueError:
            print("This should not happen. Please look into get_time()")

if __name__ == "__main__":
    pass

