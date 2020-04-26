import datetime
from pathlib import Path
from zipfile import BadZipFile

import pytest

from workbooks_processing import _process_excel_file


def test_process_excel_file():
    with (Path("resources") / "My Business Ltd.xlsx").open("rb") as file:
        assert _process_excel_file(file) == {
            "account_id": "0010g00001Z54O9",
            "actual_annual_effective_rate": 0.5687988350562374,
            "actual_daily_amortization": 47.54709239130435,
            "actual_end_date": datetime.datetime(2019, 7, 24, 0, 0),
            "amortization_period_days": 368,
            "case_number": None,
            "daily_amortization": 47.54709239130435,
            "daily_interest_rate": 0.0012344880753316536,
            "dba": "My Business Ltd",
            "discount_rate": 1.207545203588682,
            "effective_interest_rate": 0.5746160007348715,
            "expected_annual_effective_rate": 0.5687988350562374,
            "expected_end_date": datetime.datetime(2019, 7, 9, 0, 0),
            "fees_available_to_charge": -230.22839951009175,
            "fees_charged_to_date": 0,
            "funded_amount": 14055.3,
            "funding_date": datetime.datetime(2018, 7, 6, 0, 0),
            "journal_number": None,
            "loan_principal": 14490,
            "loan_specified_amount": 17497.33,
            "mid": "BV42833",
            "number_of_nsf_last_30_days": 0,
            "opportunity_id": "0060g00000uE4SU",
            "origination_fee": 434.7,
            "payment_amount": 70.55375000000001,
            "payment_interest_rate": 0.001566359596396151,
            "period_months": 12.0986301369863,
            "province": "BC",
            "repeat_balance": 0,
            "total_days": 368,
            "total_number_of_payments": 248,
            "working_days": 248,
        }


def test_process_excel_file_without_opp():
    with (Path("resources") / "My Business without Opportunity Ltd.xlsx").open(
        "rb"
    ) as file:
        assert _process_excel_file(file) == {
            "account_id": "0017000001XR9yl",
            "actual_annual_effective_rate": 0.53397497976934,
            "actual_daily_amortization": 19.696969696969695,
            "actual_end_date": datetime.datetime(2019, 12, 15, 0, 0),
            "amortization_period_days": 363,
            "case_number": None,
            "daily_amortization": 19.696969696969695,
            "daily_interest_rate": 0.0011729130576225723,
            "dba": "My Business 2 Ltd",
            "discount_rate": 1.1916666666666667,
            "effective_interest_rate": 0.5303828622986106,
            "expected_annual_effective_rate": 0.53397497976934,
            "expected_end_date": datetime.datetime(2018, 6, 28, 0, 0),
            "fees_available_to_charge": 78.0590216385696,
            "fees_charged_to_date": 0,
            "funded_amount": 5820,
            "funding_date": datetime.datetime(2017, 6, 30, 0, 0),
            "journal_number": None,
            "loan_principal": 6000,
            "loan_specified_amount": 7150,
            "mid": "BV04554",
            "none": None,
            "number_of_nsf_last_30_days": 0,
            "origination_fee": 180,
            "payment_amount": 28.830645161290324,
            "payment_interest_rate": 0.001452848116452644,
            "period_months": 11.934246575342465,
            "province": "BC",
            "repeat_balance": None,
            "total_days": 363,
            "total_number_of_payments": 248,
            "working_days": 248,
        }


def test_process_excel_file_bad_file():
    with pytest.raises(BadZipFile):
        with (Path("resources") / "Not An Excel File.xlsx").open("rb") as file:
            _process_excel_file(file)
