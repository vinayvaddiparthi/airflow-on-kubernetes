import logging
import json
import pendulum
import pandas as pd
import boto3
import sqlalchemy

from airflow import DAG
from typing import Any
from helpers.aws_hack import hack_clear_aws_keys
from sqlalchemy import Table, MetaData, VARCHAR
from sqlalchemy.sql import select, func, text, literal_column, literal, join
from concurrent.futures.thread import ThreadPoolExecutor
from utils.failure_callbacks import slack_dag
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from pyporky.symmetric import SymmetricPorky
from base64 import b64decode


def store_flinks_response(
    merchant_guid: str,
    file_path: str,
    bucket_name: str,
    snowflake_connection: str,
    schema: str,
) -> None:
    hack_clear_aws_keys()
    metadata = MetaData()
    snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()

    logging.info(f"Processing Flinks response for {merchant_guid} in {file_path}")

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=file_path)

    # This will read the whole file into memory
    encrypted_contents = json.loads(response["Body"].read())

    data = str(
        SymmetricPorky(aws_region="ca-central-1").decrypt(
            enciphered_dek=b64decode(encrypted_contents["key"], b"-_"),
            enciphered_data=b64decode(encrypted_contents["data"], b"-_"),
            nonce=b64decode(encrypted_contents["nonce"], b"-_"),
        ),
        "utf-8",
    )

    # Need to convert from a ruby hash to JSON
    data = data.replace("=>", ": ")
    data = data.replace("nil", "null")
    json_data = json.loads(data)

    logging.info(
        f"Decrypted Flinks response in {file_path}, bucket={bucket_name} ({type(json_data)})"
    )

    # Some Flinks files are now stored as arrays of transaction files
    account_data = json_data if type(json_data) is list else [json_data]

    logging.info(
        f"Processing {len(account_data)} Flinks files in {file_path}, bucket={bucket_name}"
    )

    flinks_raw_responses = Table(
        "flinks_raw_responses",
        metadata,
        autoload=True,
        autoload_with=snowflake_engine,
        schema=schema,
    )

    with snowflake_engine.begin() as tx:
        for i, data in enumerate(account_data, start=1):
            select_query = select(
                columns=[
                    literal_column(f"'{merchant_guid}'").label("merchant_guid"),
                    literal_column(f"'{response['LastModified']}'").label(
                        "batch_timestamp"
                    ),
                    literal_column(f"'{file_path}'").label("file_path"),
                    func.parse_json(json.dumps(data)).label("raw_response"),
                ]
            )

            insert_query = flinks_raw_responses.insert().from_select(
                [
                    "merchant_guid",
                    "batch_timestamp",
                    "file_path",
                    "raw_response",
                ],
                select_query,
            )

            tx.execute(insert_query)

            logging.info(
                f"✔️ Successfully stored {file_path} for {merchant_guid} ({i})"
            )


def copy_transactions(
    snowflake_connection: str,
    schema: str,
    bucket_name: str,
    num_threads: int = 4,
    **kwargs: Any,
) -> None:
    snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
    metadata = MetaData()

    merchants = Table(
        "merchants",
        metadata,
        autoload=True,
        autoload_with=snowflake_engine,
        schema=schema,
    )

    merchant_documents = Table(
        "documents",
        metadata,
        autoload=True,
        autoload_with=snowflake_engine,
        schema=schema,
    )

    flinks_raw_responses = Table(
        "flinks_raw_responses",
        metadata,
        autoload=True,
        autoload_with=snowflake_engine,
        schema=schema,
    )

    merchants_merchant_documents_join = join(
        merchant_documents,
        merchants,
        func.get(merchants.c.fields, "id")
        == func.get(merchant_documents.c.fields, "document_owner_id"),
    )
    merchant_documents_select = (
        select(
            columns=[
                sqlalchemy.cast(func.get(merchants.c.fields, "guid"), VARCHAR).label(
                    "merchant_guid"
                ),
                sqlalchemy.cast(
                    func.get(merchant_documents.c.fields, "cloud_file_path"),
                    VARCHAR,
                ).label("file_path"),
                text("1"),
            ],
            from_obj=merchant_documents,
        )
        .where(
            sqlalchemy.cast(func.get(merchant_documents.c.fields, "doc_type"), VARCHAR)
            == literal("flinks_raw_response")
        )
        .select_from(merchants_merchant_documents_join)
    )

    logging.info(
        merchant_documents_select.compile(compile_kwargs={"literal_binds": True})
    )

    flinks_raw_responses_select = select(
        columns=[
            flinks_raw_responses.c.merchant_guid,
            flinks_raw_responses.c.file_path,
            text("1"),
        ],
        from_obj=flinks_raw_responses,
    )

    logging.info(
        flinks_raw_responses_select.compile(compile_kwargs={"literal_binds": True})
    )

    # Get the set of all the raw flinks responses
    all_flinks_responses = pd.read_sql_query(
        merchant_documents_select,
        snowflake_engine,
        index_col=[
            "merchant_guid",
            "file_path",
        ],
    )

    # Get the set of downloaded flinks responses
    downloaded_flinks_responses = pd.read_sql_query(
        flinks_raw_responses_select,
        snowflake_engine,
        index_col=[
            "merchant_guid",
            "file_path",
        ],
    )

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for _index, row in all_flinks_responses.iterrows():
            try:
                # See if we already have it
                downloaded_flinks_responses.loc[
                    (
                        row.name[0],
                        row.name[1],
                    )
                ]

                # We already have it
                logging.info(
                    f"⏩️️ Skipping generating projections for {row.name[0]} - {row.name[1]}"
                )
            except KeyError:
                # We don't have it
                executor.submit(
                    store_flinks_response,
                    row.name[0],
                    row.name[1],
                    bucket_name,
                    snowflake_connection,
                    schema,
                )


def create_dag() -> DAG:
    with DAG(
        "process_flinks_transactions",
        max_active_runs=10,
        schedule_interval="0 4 * * *",
        start_date=pendulum.datetime(
            2020, 8, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        on_failure_callback=slack_dag("slack_data_alerts"),
        default_args={"retries": 5, "retry_delay": timedelta(minutes=2)},
    ) as dag:
        (
            dag
            << PythonOperator(
                task_id="copy_transactions",
                python_callable=copy_transactions,
                provide_context=True,
                op_kwargs={
                    "snowflake_connection": "airflow_production_test",
                    "schema": "CORE_PRODUCTION",
                    "bucket_name": "ario-documents-production",
                    "num_threads": 10,
                },
                executor_config={
                    "KubernetesExecutor": {
                        "annotations": {
                            "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                            "KubernetesAirflowProductionFlinksRole"
                        }
                    }
                },
            )
            >> DbtOperator(
                task_id="dbt_run_process_transactions",
                execution_timeout=timedelta(hours=1),
                action=DbtAction.run,
                models=(
                    "fct_bank_account_transaction fct_daily_bank_account_balance "
                    "fct_weekly_bank_account_balance fct_monthly_bank_account_balance "
                    "fct_bank_account_balance_week_over_week fct_bank_account_balance_month_over_month"
                ),
            )
        )

        return dag


globals()["process_flinks_transactions"] = create_dag()
