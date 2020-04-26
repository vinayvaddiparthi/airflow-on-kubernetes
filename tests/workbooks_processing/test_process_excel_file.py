import datetime
from pathlib import Path
from zipfile import BadZipFile

import pytest

from workbooks_processing import _process_excel_file


def test_process_excel_file():
    with (Path("resources") / "My Business Ltd.xlsx").open("rb") as file:
        assert _process_excel_file(file) == {
            "account id": "0010g00001Z54O9",
            "actual annual effective rate": 0.5687988350562374,
            "actual daily amortization": 47.54709239130435,
            "actual end date": datetime.datetime(2019, 7, 24, 0, 0),
            "amortization period (days)": 368,
            "case number": None,
            "daily amortization": 47.54709239130435,
            "daily interest rate": 0.0012344880753316536,
            "dba": "My Business Ltd",
            "discount rate": 1.207545203588682,
            "effective interest rate": 0.5746160007348715,
            "expected annual effective rate": 0.5687988350562374,
            "expected end date": datetime.datetime(2019, 7, 9, 0, 0),
            "fees available to charge": -230.22839951009175,
            "fees charged to date": 0,
            "funded amount": 14055.3,
            "funding date": datetime.datetime(2018, 7, 6, 0, 0),
            "journal number": None,
            "loan principal": 14490,
            "loan specified amount": 17497.33,
            "mid": "BV42833",
            "number of nsf (last 30 days)": 0,
            "opportunity id": "0060g00000uE4SU",
            "origination fee": 434.7,
            "payment amount": 70.55375000000001,
            "payment interest rate": 0.001566359596396151,
            "period (months)": 12.0986301369863,
            "province": "BC",
            "repeat balance": 0,
            "total days": 368,
            "total number of payments": 248,
            "working days": 248,
        }


def test_process_excel_file_without_opp():
    with (Path("resources") / "My Business without Opportunity Ltd.xlsx").open(
        "rb"
    ) as file:
        assert _process_excel_file(file) == {
            "account id": "0017000001XR9yl",
            "actual annual effective rate": 0.53397497976934,
            "actual daily amortization": 19.696969696969695,
            "actual end date": datetime.datetime(2019, 12, 15, 0, 0),
            "amortization period (days)": 363,
            "case number": None,
            "daily amortization": 19.696969696969695,
            "daily interest rate": 0.0011729130576225723,
            "dba": "My Business 2 Ltd",
            "discount rate": 1.1916666666666667,
            "effective interest rate": 0.5303828622986106,
            "expected annual effective rate": 0.53397497976934,
            "expected end date": datetime.datetime(2018, 6, 28, 0, 0),
            "fees available to charge": 78.0590216385696,
            "fees charged to date": 0,
            "funded amount": 5820,
            "funding date": datetime.datetime(2017, 6, 30, 0, 0),
            "journal number": None,
            "loan principal": 6000,
            "loan specified amount": 7150,
            "mid": "BV04554",
            "none": None,
            "number of nsf (last 30 days)": 0,
            "origination fee": 180,
            "payment amount": 28.830645161290324,
            "payment interest rate": 0.001452848116452644,
            "period (months)": 11.934246575342465,
            "province": "BC",
            "repeat balance": None,
            "total days": 363,
            "total number of payments": 248,
            "working days": 248,
        }


def test_process_excel_file_bad_file():
    with pytest.raises(BadZipFile):
        with (Path("resources") / "Not An Excel File.xlsx").open("rb") as file:
            _process_excel_file(file)
