from dags.equifax_extras.consumer.validation import (
    check_len,
    check_footer,
    check_city,
    check_sin,
    check_header,
    check_dob,
    check_postal_code,
    check_province,
)


def test_check_len():
    line = "someteststring"
    line2 = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901"
    assert check_len(line) == len(line)
    assert not check_len(line2)


def test_check_header():
    header = "BHDR-EQUIFAX20201103191609ADVITFINSCOREDA2\n"
    header1 = "BHDR-EQUIFAX20201103191609ADVITFINSCOREDA2"
    header2 = "BTRL-EQUIFAX20201103191609ADVITFINSCOREDA2\n"
    header3 = "BHDR-EQUIFAX20201103191609ADVITFINSCOREDA3\n"
    assert not check_header(header)
    assert check_header(header1) == f"length error: {len(header1)}"
    assert check_header(header2) == f"prefix error: {header2}"
    assert check_header(header3) == f"suffix error: {header3}"


def test_check_footer():
    count = 10
    footer = "BTRL-EQUIFAX20201103191609ADVITFINSCOREDA200000010"
    footer1 = "BTRL-EQUIFAY20201103191609ADVITFINSCOREDA200000010"
    footer2 = "BTRL-EQUIFAX20201103191609ADVITFINSCOREDA300000010"
    footer3 = "BTRL-EQUIFAX20201103191609ADVITFINSCOREDA200000010 "
    footer4 = "BTRL-EQUIFAX20201103191609ADVITFINSCOREDA200000011"
    assert not check_footer(footer, count)
    assert check_footer(footer1, count) == f"prefix error: {footer1}"
    assert check_footer(footer2, count) == f"suffix error: {footer2}"
    assert check_footer(footer3, count) == f"length error: 51"
    assert check_footer(footer4, count) == f"count error: 11"


def test_check_dob():
    line = "122256"
    line1 = "000056"
    line2 = "123256"
    line3 = "132256"
    assert not check_dob(line)
    assert check_dob(line1) == line1
    assert check_dob(line2) == line2
    assert check_dob(line3) == line3


def test_check_sin():
    line = "999999999"
    line1 = "         "
    line2 = "123456asd"
    assert not check_sin(line)
    assert not check_sin(line1)
    assert check_sin(line2) == line2


def test_check_postal():
    line = "h3h1h1"
    line1 = "      "
    line2 = "h3h11h"
    assert not check_postal_code(line)
    assert not check_postal_code(line1)
    assert check_postal_code(line2) == line2


def test_check_city():
    line = "mtl"
    line1 = " "
    assert not check_city(line)
    assert check_postal_code(line1) == line1


def test_check_province():
    line = "qc"
    line1 = " "
    line2 = "q3"
    assert not check_province(line)
    assert check_province(line1) == line1
    assert check_province(line2) == line2
