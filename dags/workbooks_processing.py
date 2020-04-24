import os
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from openpyxl import load_workbook
import boto3

from utils import sf15to18, random_identifier


def _process_excel_file(bucket, obj):
    print(f"⚙️ Processing {obj}...", sep=" ")

    with tempfile.TemporaryFile() as f:
        bucket.download_fileobj(obj.key, f)

        try:
            workbook = load_workbook(filename=f, data_only=True, read_only=True)
            ws = workbook["Calculation Sheet"]
            calc_sheet = {
                k[0].value: v[0].value for k, v in zip(ws["A1":"A32"], ws["B1":"B32"])
            }
            return calc_sheet
        except Exception as e:
            print(f"❌ Error processing {obj}: {e}")


def import_workbooks(
    bucket: str,
    snowflake_conn: str,
    destination_schema: str,
    destination_table: str,
    num_threads: int = 128,
):
    # Delete AWS credentials used to upload logs from this context
    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    s3 = boto3.resource("s3")

    stage_guid = random_identifier()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(_process_excel_file, bucket, obj)
            for obj in s3.Bucket(name=bucket).objects.all()
            if (obj.key).lower().endswith(".xlsx") and not "~" in obj.key
        ]

    df = pd.DataFrame([future.result() for future in futures])
    print(f"Done processing {len(futures)} workbooks")
    df["Account ID - 18"] = df.apply(lambda row: sf15to18(row["Account ID"]), axis=1)
    print(f"Done computing 18 characters SFDC object IDs")

    engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    with engine_.begin() as tx, tempfile.NamedTemporaryFile() as path:
        df.to_parquet(f"{path}", engine="fastparquet", compression="gzip")
        print("Dataframe converted to parquet")

        stmts = [
            f"CREATE OR REPLACE TEMPORARY STAGE {destination_schema}.{stage_guid} FILE_FORMAT=(TYPE=PARQUET)",
            f"PUT file://{path} @{destination_schema}.{stage_guid}",
            f"CREATE OR REPLACE TRANSIENT TABLE {destination_schema}.{destination_table} AS SELECT * FROM @{destination_schema}.{stage_guid}",
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
