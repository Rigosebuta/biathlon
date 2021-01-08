import fitz
import pytest
from DataProcessing import converting_data as cv


@pytest.fixture
def pdf_doc():
    return fitz.Document("../TestData/BT_C51A_1.0(1).pdf")


@pytest.mark.parametrize("type_error_convert_to_pdf", [
    None, 1, 0, 1.5, [1, 2, 3], {}, [pdf_doc, 1, "Bye"]
])
def test_convert_pdf_to_document_1(type_error_convert_to_pdf):
    """This method tests the method convert_pdf_to_document() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.convert_pdf_to_document(type_error_convert_to_pdf)


@pytest.mark.parametrize("file_exist_convert_to_pdf", [
    ["Hallo"], ["../DataProcessing/biathlete.py"]
])
def test_convert_pdf_to_document_2(file_exist_convert_to_pdf):
    """This method tests the method convert_pdf_to_document() with cases which invoke a FileExistsError"""
    with pytest.raises(FileExistsError):
        cv.convert_pdf_to_document(file_exist_convert_to_pdf)


def test_convert_pdf_to_document_3():
    """This method tests the method convert_pdf_to_document() with a true case"""
    assert cv.convert_pdf_to_document(["../TestData/BT_C51A_1.0(1).pdf"])


def test_convert_pdf_to_document_4():
    """This method tests the method convert_pdf_to_document() with a case which invokes a RuntimeError"""
    with pytest.raises(RuntimeError):
        cv.convert_pdf_to_document(["Hallo.pdf"])


@pytest.mark.parametrize("type_error_filter_doc", [
    None, 1, 0, 1.5, [1, 2, 3], {}, [pdf_doc, 1, "Bye"]
])
def test_filter_doc_1(type_error_filter_doc):
    """This method tests the method filter_doc() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.filter_doc(type_error_filter_doc)


def test_filter_doc_2(pdf_doc):
    """This method tests the method filter_doc() with a true case"""
    assert cv.filter_doc([pdf_doc])


@pytest.mark.parametrize("type_error_is_not_pdf", [
    None, 1, 0, 1.5, [1, 2, 3], {}, [pdf_doc, 1, "Bye"]
])
def test_is_not_pdf_1(type_error_is_not_pdf):
    """This method tests the method is_not_pdf() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.is_not_pdf(type_error_is_not_pdf)


def test_is_not_pdf_2(pdf_doc):
    """This method tests the method filter_doc() with a true case"""
    assert not cv.is_not_pdf([pdf_doc])


@pytest.mark.parametrize("type_error_divide_into_pages", [
    None, 1, 0, 1.5, [1, 2, 3], {}, "bye"
])
def test_divide_into_pages_1(type_error_divide_into_pages):
    """This method tests the method divide_into_pages() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.divide_into_pages(type_error_divide_into_pages)


def test_divide_into_pages_2(pdf_doc):
    """This method tests if the returned list has the same number of pages as the former pdf document"""
    assert cv.divide_into_pages(pdf_doc)
    assert len(cv.divide_into_pages(pdf_doc)) == pdf_doc.pageCount


@pytest.mark.parametrize("type_error_from_celsius_to_float", [
    None, 1, 1.5, [1, 2, 3], [1, 2, 3, 4], {}, "bye", ["A", "B", "C", "D"], ["°", "°", "°", "°"],
    ["1,2°", "1,°", "1.2°", "1,2°"], [".2°", "3.2°", "32.2°", "32.1°"], ["32.2°", "32.2°", "2.3°3.2", "32.2°"],
    ["32.2°", "32.2°", "2.3°3.2", "32.2°"], ["32.2°", "32.2°", "223.32°", "32.2°"],
    ["32.2°", "32.2°", "2.3", "32.223°"]
])
def test_from_one_unity_to_float_1(type_error_from_celsius_to_float):
    """This method tests the method from_celsius_to_float() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.from_one_unity_to_float(type_error_from_celsius_to_float, "°")


def test_from_one_unity_to_float_2():
    """This method tests the method from_one_unity_to_float() with a true case"""
    assert cv.from_one_unity_to_float(["3.2°", "3.2°", "32.2°", "32.1°"], "°")


@pytest.mark.parametrize("type_error_from_meter_per_s_to_float", [
    None, 1, 1.5, [1, 2, 3], [1, 2, 3, 4], {}, "bye", ["A", "B", "C", "D"], [" ", " ", " ", " "],
    ["1,2 ", "1, ", "1.2 ", "1,2 "], [".2", "3.2", "32.2", "32.1"], ["32.2", "32.2", "2.33.2", "32.2"],
    ["32.2", "32.2", "2.33.2", "32.2"], ["32.2", "32.2", "223.32", "32.2"],
    ["32.2", "32.2", "2.3", "32.223"], ["32.2 m/s", "32.2 m/s", "2.3 m/s", "32. 22 m/s"],
    ["S 32.2 m/s", "SW32.2 m/s", "N2.3 m/s", "W32. 22 m/s"]
])
def test_from_one_unity_to_float_3(type_error_from_meter_per_s_to_float):
    """This method tests the method from_celsius_to_float() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.from_one_unity_to_float(type_error_from_meter_per_s_to_float, " ")


def test_from_one_unity_to_float_4():
    """This method tests the method from_one_unity_to_float() with a true case"""
    assert cv.from_one_unity_to_float(["32.2 m/s", "32.2 m/s", "2.3 m/s", "32.22 m/s"], " ")


@pytest.mark.parametrize("type_error_eliminating_leading_zero", [
    0, 1, 1.5, [1, 2, 3], [1, 2, 3, 4], {}, "", (2, 3, 4), ()
])
def test_eliminating_leading_zero_1(type_error_eliminating_leading_zero):
    """This method tests the method eliminating_leading_zero() with cases which invoke a TypeError"""
    with pytest.raises(TypeError):
        cv.eliminating_leading_zero(type_error_eliminating_leading_zero)


def test_eliminating_leading_zero_2():
    """This method tests the method eliminating_leading_zero() with a true case"""
    assert cv.eliminating_leading_zero("bye") == "bye"
    assert cv.eliminating_leading_zero("0bye") == "0bye"[1:]
