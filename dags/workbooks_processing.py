import logging
import os
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from openpyxl import load_workbook
import boto3
from slugify import slugify

from utils import sf15to18, random_identifier


def _wrap_sf15to18(id: str) -> Optional[str]:
    try:
        return sf15to18(id)
    except Exception as e:
        logging.error(f"❌ Error converting SFDC ID {id} from 15 to 18 chars: {e}")
        return None


def _process_excel_file(file):
    workbook = load_workbook(filename=file, data_only=True, read_only=True)
    ws = workbook["Calculation Sheet"]
    calc_sheet = {
        slugify(str(k[0].value), separator="_"): v[0].value
        for k, v in zip(ws["A1":"A65535"], ws["B1":"B65535"])
        if k and v
    }
    return calc_sheet


def _get_and_process_workbook(bucket, obj):
    try:
        with tempfile.TemporaryFile() as f:
            bucket.download_fileobj(obj.key, f)
            _process_excel_file(f)
    except Exception as e:
        logging.error(f"❌ Error processing {obj}: {e}")
        return {}


def import_workbooks(
    bucket: str,
    snowflake_conn: str,
    destination_schema: str,
    destination_table: str,
    num_threads: int = 4,
):
    # Delete AWS credentials used to upload logs from this context
    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except Exception:  # nosec
        pass

    s3 = boto3.resource("s3")
    bucket_ = s3.Bucket(name=bucket)

    stage_guid = random_identifier()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        print(f"⚙️ Processing {bucket_}...")
        futures = [
            executor.submit(_get_and_process_workbook, bucket_, obj)
            for obj in bucket_.objects.all()
            if (obj.key).lower().endswith(".xlsx") and not "~" in obj.key
        ]

    df = pd.DataFrame([future.result() for future in futures])
    print(f"✔️ Done processing {len(futures)} workbooks")
    df["account_id_18"] = df.apply(
        lambda row: _wrap_sf15to18(row.get("account_id")), axis=1
    )
    print(f"✔️ Done computing 18 characters SFDC object IDs")

    engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    with engine_.begin() as tx, tempfile.NamedTemporaryFile("w") as file:
        df.to_csv(path_or_buf=file)
        print("Dataframe converted to CSV")

        stmts = [
            f"CREATE OR REPLACE TEMPORARY STAGE {destination_schema}.{stage_guid} FILE_FORMAT=(TYPE=CSV)",  # nosec
            f"PUT file://{file.name} @{destination_schema}.{stage_guid}",  # nosec
            f"CREATE OR REPLACE TRANSIENT TABLE {destination_schema}.{destination_table} AS SELECT * FROM @{destination_schema}.{stage_guid}",  # nosec
        ]

        print("Uploading results to Snowflake ❄️")
        return [tx.execute(stmt).fetchall() for stmt in stmts]


with DAG(
    dag_id="workbooks_processing",
    start_date=pendulum.datetime(
        2020, 4, 24, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval=None,
) as dag:
    dag << PythonOperator(
        task_id="process_workbooks",
        python_callable=import_workbooks,
        op_kwargs={
            "bucket": "tc-workbooks",
            "snowflake_conn": "snowflake_tclegacy",
            "destination_schema": "PUBLIC",
            "destination_table": "WORKBOOKS",
            "num_threads": 10,
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowProductionWorkbooksRole"
                }
            }
        },
    )
