import os

import boto3
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from dags.zetatango_import import decrypt_pii_columns, DecryptionSpec


def test_pii_decryption(mocker):
    boto3.DEFAULT_SESSION = boto3.session.Session(profile_name="prod-cmk-user")

    mocker.patch(
        "dags.zetatango_import.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            URL(
                account="thinkingcapital.ca-central-1.aws",
                user=os.environ["SNOWFLAKE_USERNAME"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                database="ZETATANGO",
                warehouse="ETL",
            )
        ),
    )

    decrypt_pii_columns(
        "abc",
        [
            DecryptionSpec(
                schema="CORE_PRODUCTION",
                table="LENDING_ADJUDICATIONS",
                columns=["offer_results", "adjudication_results", "notes"],
                format=["yaml", "yaml", None],
            )
        ],
        target_schema="PII_PRODUCTION",
    )
