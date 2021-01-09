"""This module convert files into 'Document' objects

    - For more information about the PyMuPDF package and
    therefore fitz: https://pymupdf.readthedocs.io/en/latest/
    - For more information about the 'Document' class
    from PyMuPDF: https://pymupdf.readthedocs.io/en/latest/document.html
"""

import re
import fitz


def convert_pdf_to_document(file_list):
    """This method converts a list of strings (filenames) into a list of 'Document' objects.

        :arg:
            file_list (list): list of file names as strings
        :return:
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
    """This method returns a list of 'Document' objects which are connected to pdfs.

        :arg:
            doc_ls (list): a list of 'Document' objects
        :return:
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
    """This method returns a list of 'Document' objects which aren't connected to pdfs.

        :arg:
            doc_ls (list): a list of 'Document' objects
        :return:
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
    """This method divides every 'Document' (connected to a pdf) into pages.

        :arg:
            pdf_doc (Document): a 'Document' object
        :return:
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
    """This method transforms a string (degree in celsius) list into a float list.

        :arg:
            unity_ls (str list): list with strings in celsius-in-degree format
            sep: separator for the string
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
            raise TypeError('This should not happen.'
                            'Please look into converting_data.from_one_unity_to_float()')
        else:
            matches_str = "".join(matches)  # convert list to string
            matches_str = matches_str.replace(" ", "")
            matches_str = matches_str.replace(",", ".")
            matches_float = float(matches_str)
        if matches:
            float_ls.append(matches_float)
        else:
            raise TypeError("This should not happen."
                            "Please look into converting_data.from_one_unity_to_float")
    return float_ls


def eliminating_leading_zero(number_as_string):
    """This method eliminates leading zeros of a string of max. length 2.

        :arg:
            number_as_strings (string): processed string
        :return:
            the input string without leading zeros
    """
    if not isinstance(number_as_string, str):
        raise TypeError
    if len(number_as_string) == 0:
        raise TypeError
    if number_as_string[0] == '0':
        return number_as_string[1:]
    return number_as_string


if __name__ == "__main__":
    pass

