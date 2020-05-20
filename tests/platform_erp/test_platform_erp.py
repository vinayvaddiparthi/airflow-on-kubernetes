import os
from datetime import datetime
from zeep import Client
import pandas as pd

from platform_journal_entry_dag import build_journal_entry


def test_build_journal_entry():
    time = datetime.utcnow()
    correlation_guid = "correlation_guid1"
    amount = 123
    trx = {
        "correlation_guid": [correlation_guid, correlation_guid],
        "posted_at": [time, time],
        "credit_amount": [0, amount],
        "debit_amount": [amount, 0],
        "account_number": [13000, 14300],
        "facility": [1, 3],
        "ns_account_internal_id": [22000, 25000],
        "ns_subsidiary_id": [6, 6],
    }

    grouped_transactions = pd.DataFrame(
        trx,
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
    client = Client(os.environ["ns_wsdl_sb"])
    created_at = "2020-01-01"
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
