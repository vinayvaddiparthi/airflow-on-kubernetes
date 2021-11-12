import logging
import json
import tempfile
from typing import Any
from pathlib import Path
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

from utils import random_identifier
from utils.failure_callbacks import slack_dag
from google_analytics_import import (
    initialize_analytics_reporting,
    get_report,
    next_page_token,
    build_deduplicate_query,
    transform_raw_json,
)
from data.google_analytics import reports

with DAG(
    dag_id="historical_google_analytics_import",
    max_active_runs=1,
    schedule_interval=None,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=True,
    start_date=pendulum.datetime(
        2021, 11, 4, tzinfo=pendulum.timezone("America/Toronto")
    ),
    on_failure_callback=slack_dag("slack_data_alerts"),
) as dag:

    def process(
        table: str, conn: str, start_date: str, end_date: str, **context: Any
    ) -> None:
        ds = context["ds"]
        logging.info(f"Date Range: {start_date} - {end_date}")
        analytics = initialize_analytics_reporting()
        google_analytics_hook = BaseHook.get_connection("google_analytics_snowflake")
        dest_db = google_analytics_hook.extra_dejson.get("dest_db")
        dest_schema = google_analytics_hook.extra_dejson.get("dest_schema")

        with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
            stage_guid = random_identifier()
            tx.execute(f"use database {dest_db}")
            tx.execute(
                f"create or replace stage {dest_schema}.{stage_guid} file_format=(type=json)"
            ).fetchall()
            logging.info(
                f"create or replace stage {dest_schema}.{stage_guid} "
                f"file_format=(type=json)"
            )
            logging.info("Initialize page_token")
            page_token: Any = "0"
            while page_token:
                response = get_report(
                    analytics, table, start_date, end_date, page_token
                )
                if response:
                    res_json = transform_raw_json(response, ds)
                    with tempfile.TemporaryDirectory() as tempdir:
                        json_filepath = Path(
                            tempdir, f"{table}{page_token}"
                        ).with_suffix(".json")
                        for i in range(len(res_json)):
                            with open(json_filepath, "a") as outfile:
                                outfile.writelines(json.dumps(res_json[i]))
                        tx.execute(
                            f"put file://{json_filepath} @{dest_schema}.{stage_guid}"
                        ).fetchall()
                    logging.info(f"{table} row count: {len(res_json)}")
                    token = next_page_token(response)
                if token:
                    page_token = str(token)
                else:
                    page_token = None

            tx.execute(
                f"create or replace table {dest_db}.{dest_schema}.{table}_stage as "  # nosec
                f"select $1 as fields from @{dest_schema}.{stage_guid}"  # nosec
            )

            # back up server_cx
            tx.execute(
                f"create table {dest_db}.{dest_schema}.{table}_backup20211112 "  # nosec
                f"clone {dest_db}.{dest_schema}.{table}"  # nosec
            )

            if "primary_keys" in reports[table]:  # type: ignore
                tx.execute(build_deduplicate_query(dest_db, dest_schema, table))
            tx.execute(
                f"insert into {dest_db}.{dest_schema}.{table} "  # nosec
                f"select * from {dest_db}.{dest_schema}.{table}_stage"  # nosec
            )
            tx.execute(f"drop table {dest_db}.{dest_schema}.{table}_stage")

            # clean email events imported in 2021-10-28
            tx.execute(
                f"delete from {dest_db}.{dest_schema}.{table} "  # nosec
                f"where fields:batch_import_date::string='2021-11-12' and fields:eventCategory::string='email'"  # nosec
            )

            logging.info(
                f"✔️ Successfully loaded missing server_cx records for {start_date} - {end_date} on {ds}"
            )

    dag << PythonOperator(
        task_id="import_missing_server_cx",
        python_callable=process,
        op_kwargs={
            "conn": "snowflake_production",
            "table": "server_cx",
            "start_date": "2021-07-09",
            "end_date": "2021-10-28",
        },
        provide_context=True,
    )
