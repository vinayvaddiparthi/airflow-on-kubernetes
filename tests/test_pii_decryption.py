import os

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from dags.zetatango_import import decrypt_pii_columns, DecryptionSpec


def test_pii_decryption(mocker):
    hook_mock = mocker.patch(
        "dags.zetatango_import.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            URL(
                account="thinkingcapital.ca-central-1.aws",
                user="ZETATANGO_ELT_STAGING",
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
                schema="CORE_STAGING", table="MERCHANT_ATTRIBUTES", columns=["value"]
            )
        ],
    )
