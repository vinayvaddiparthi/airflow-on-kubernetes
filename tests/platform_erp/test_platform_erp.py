import os
from datetime import datetime

import pendulum
import pytest
from zeep import Client
import pandas as pd

from platform_journal_entry_dag import (
    build_journal_entry,
    process_grouped_transactions,
    add_time_zone,
)

client = Client(os.environ["ns_wsdl_sb"])
passport_type = client.get_type("ns0:Passport")
passport = passport_type(
    email=os.environ["email"],
    password=os.environ["password"],
    account=os.environ["account"],
)

applicationinfo_type = client.get_type("ns4:ApplicationInfo")
app_info = applicationinfo_type(applicationId=os.environ["app_id"])
time = datetime.utcnow()
correlation_guid = "correlation_guid_test"
amount = 1
transaction_invalid_key = {
    "correlation_guid": [correlation_guid, correlation_guid],
    "posted_at": [time, time],
    "credit_amount": [0, amount],
    "debit_amount": [amount, 0],
    "account_number": [13000, 14300],
    "facility": [1, 3],
    "ns_account_internal_id": [22000, 25000],
    "ns_subsidiary_id": [6, 6],
}
transaction_subsidiary_diff = {
    "correlation_guid": [correlation_guid, correlation_guid],
    "posted_at": [time, time],
    "credit_amount": [0, amount],
    "debit_amount": [amount, 0],
    "account_number": [13000, 14300],
    "facility": [1, 3],
    "ns_account_internal_id": [22000, 25000],
    "ns_subsidiary_id": [11, 6],
}
transaction_dup = {
    "correlation_guid": [correlation_guid, correlation_guid],
    "posted_at": [time, time],
    "credit_amount": [0, amount],
    "debit_amount": [amount, 0],
    "account_number": [13000, 11500],
    "facility": [1, 1],
    "ns_account_internal_id": [950, 257],
    "ns_subsidiary_id": [6, 6],
}

created_at = "2020-01-01"


def test_add_time_zone():
    time_zone = "America/Toronto"
    date_value1 = "2020-07-20T21:00:00"
    date_value2 = "2020-07-20 21:00:00"
    date_value3 = datetime.strptime("2020-07-20 21:00:00", "%Y-%m-%d %H:%M:%S")
    assert add_time_zone(date_value1) == pendulum.from_format(
        "2020-07-20 21:00:00", "%Y-%m-%d %H:%M:%S", tz=time_zone
    )
    assert add_time_zone(date_value2) == pendulum.from_format(
        "2020-07-20 21:00:00", "%Y-%m-%d %H:%M:%S", tz=time_zone
    )
    assert add_time_zone(date_value3) == pendulum.from_format(
        "2020-07-20 21:00:00", "%Y-%m-%d %H:%M:%S", tz=time_zone
    )


def test_process_grouped_transactions_dup():
    grouped_transactions = pd.DataFrame(
        transaction_dup,
        columns=[
            "correlation_guid",
            "posted_at",
            "credit_amount",
            "debit_amount",
            "account_number",
            "facility",
            "ns_account_internal_id",
            "ns_subsidiary_id",
        ],
    )
    try:
        process_grouped_transactions(
            correlation_guid,
            grouped_transactions,
            client,
            passport,
            app_info,
            created_at,
        )
    except Exception as e:
        print(e)
        result = e.args[0]
    assert result.statusDetail[0].code == "DUP_RCRD"


def test_process_grouped_transactions_invalid_key():
    grouped_transactions = pd.DataFrame(
        transaction_invalid_key,
        columns=[
            "correlation_guid",
            "posted_at",
            "credit_amount",
            "debit_amount",
            "account_number",
            "facility",
            "ns_account_internal_id",
            "ns_subsidiary_id",
        ],
    )
    try:
        process_grouped_transactions(
            correlation_guid,
            grouped_transactions,
            client,
            passport,
            app_info,
            created_at,
        )
    except Exception as e:
        print(e)
        result = e.args[0]
    assert result.statusDetail[0].code == "INVALID_KEY_OR_REF"


def test_process_grouped_transactions_subsidiary_diff():
    grouped_transactions = pd.DataFrame(
        transaction_subsidiary_diff,
        columns=[
            "correlation_guid",
            "posted_at",
            "credit_amount",
            "debit_amount",
            "account_number",
            "facility",
            "ns_account_internal_id",
            "ns_subsidiary_id",
        ],
    )
    with pytest.raises(Exception, match="Different subsidiary"):
        process_grouped_transactions(
            correlation_guid,
            grouped_transactions,
            client,
            passport,
            app_info,
            created_at,
        )


def test_build_journal_entry():
    grouped_transactions = pd.DataFrame(
        transaction_invalid_key,
        columns=[
            "correlation_guid",
            "posted_at",
            "credit_amount",
            "debit_amount",
            "account_number",
            "facility",
            "ns_account_internal_id",
            "ns_subsidiary_id",
        ],
    )
    result = build_journal_entry(
        correlation_guid, grouped_transactions, client, created_at
    )
    assert 6 == result.subsidiary.internalId
    for line in result.lineList.line:
        if line.account.internalId == 22000:
            assert line.debit == amount
        if line.account.internalId == 25000:
            assert line.credit == amount
    assert f"Platform Transaction - {created_at}" == result.memo
    assert time.strftime("%Y-%m-%d") == result.tranDate.split("T")[0]
