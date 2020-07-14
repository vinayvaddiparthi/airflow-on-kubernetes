import os
import tempfile
import sys
import logging
import json
import pendulum
import pandas as pd
import boto3

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from concurrent.futures.thread import ThreadPoolExecutor
from pyporky.symmetric import SymmetricPorky
from base64 import b64decode
from utils import random_identifier
from pathlib import Path


def create_table(snowflake_connection: str):
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        create_table_query = """
CREATE TABLE IF NOT EXISTS
    "ZETATANGO"."CORE_PRODUCTION"."FLINKS_RAW_RESPONSES"
        (
            BATCH_TIMESTAMP DATETIME NOT NULL,
            FILE_PATH VARCHAR NOT NULL,
            RAW_RESPONSE VARIANT NOT NULL
        )
"""
        with snowflake_engine.begin() as tx:
            tx.execute(create_table_query).fetchall()
    except:
        e = sys.exc_info()
        logging.error(f"❌ Error creating table: {e}")
    finally:
        snowflake_engine.dispose()


def store_flinks_response(file_path, bucket_name, snowflake_connection):
    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()

        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=file_path)

        # This will read the whole file into memory
        encrypted_contents = json.loads(response["Body"].read())

        data = str(
            porky_lib.decrypt(
                enciphered_dek=b64decode(encrypted_contents["key"], "-_"),
                enciphered_data=b64decode(encrypted_contents["data"], "-_"),
                nonce=b64decode(encrypted_contents["nonce"], "-_"),
            ),
            "utf-8",
        )

        # Need to convert from a ruby hash to JSON
        data = data.replace("=>", ": ")
        data = data.replace("nil", "null")

        with snowflake_engine.begin() as tx, tempfile.TemporaryDirectory() as path:
            file_identifier = random_identifier()

            tmp_file_path = Path(path) / file_identifier
            staging_location = f'"ZETATANGO"."CORE_PRODUCTION"."{file_identifier}"'

            with open(tmp_file_path, "w") as file:
                file.write(data)

            statements = [
                f"CREATE OR REPLACE TEMPORARY STAGE {staging_location} FILE_FORMAT=(TYPE=JSON)",
                f"PUT file://{tmp_file_path} @{staging_location}",
                (
                    f'INSERT INTO "ZETATANGO"."CORE_PRODUCTION"."FLINKS_RAW_RESPONSES"'
                    f" (batch_timestamp, file_path, raw_response)"
                    f" SELECT {response['LastModified']}', '{file_path}',"
                    f" PARSE_JSON($1) AS FIELDS FROM @{staging_location}"
                ),
            ]

            for statement in statements:
                tx.execute(statement).fetchall()

            os.remove(tmp_file_path)

        logging.info(f"✔️ Successfully stored {file_path}")
    except:
        e = sys.exc_info()
        logging.error(
            f"❌ Error copying file {file_path} from {bucket_name} to Snowflake: {e}"
        )
    finally:
        snowflake_engine.dispose()


def copy_transactions(
    snowflake_connection: str, bucket_name: str, num_threads: int = 4
):
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        merchant_documents_query = """
SELECT
    fields:cloud_file_path::VARCHAR AS file_path
FROM "ZETATANGO"."CORE_PRODUCTION"."MERCHANT_DOCUMENTS"
WHERE fields:doc_type::VARCHAR = 'flinks_raw_response'
"""

        # Get the set of all the raw flinks responses
        all_flinks_responses = pd.read_sql_query(
            merchant_documents_query, snowflake_engine
        )

        flinks_raw_response_select_sql = """
SELECT
    file_path
FROM "ZETATANGO"."CORE_PRODUCTION"."FLINKS_RAW_RESPONSES"
"""

        # Get the set of downloaded flinks responses
        downloaded_flinks_responses = pd.read_sql_query(
            flinks_raw_response_select_sql, snowflake_engine
        )

        # The set difference is what we need to downloaded. file_paths are unique so this is a safe operation
        to_download = all_flinks_responses[
            ~all_flinks_responses["file_path"].isin(
                downloaded_flinks_responses["file_path"]
            )
        ]

        for _index, row in to_download.iterrows():
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                executor.submit(
                    store_flinks_response,
                    row["file_path"],
                    bucket_name,
                    snowflake_connection,
                )

    except:
        e = sys.exc_info()
        logging.error(f"❌ Error processing flinks transaction documents to copy: {e}")
    finally:
        snowflake_engine.dispose()


with DAG(
    dag_id="raw_flinks_transactions_import",
    start_date=pendulum.datetime(
        2020, 7, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="30 0,10-22/4 * * *",
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:
    dag << PythonOperator(
        task_id="create_table",
        python_callable=create_table,
        op_kwargs={"snowflake_connection": "snowflake_zetatango_production",},
    ) >> PythonOperator(
        task_id="copy_transactions",
        python_callable=copy_transactions,
        op_kwargs={
            "snowflake_connection": "snowflake_zetatango_production",
            "bucket_name": "ario-documents-production",
            "num_threads": 10,
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/KubernetesAirflowProductionFlinksRole"
                }
            }
        },
    )
