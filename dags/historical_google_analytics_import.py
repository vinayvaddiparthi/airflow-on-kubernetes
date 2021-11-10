import logging
import json
import tempfile
from typing import Any, Dict
from pathlib import Path
import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

from utils import random_identifier
from utils.failure_callbacks import slack_dag
from data.google_analytics import reports
from google_analytics_import import (
    initialize_analytics_reporting,
    get_report,
    next_page_token,
)


def transform_raw_json(raw: Dict, ds: str) -> Any:
    for report in raw.get("reports", []):
        column_header = report.get("columnHeader", {})
        dimension_headers = column_header.get("dimensions", [])
        metric_headers = column_header.get("metricHeader", {}).get(
            "metricHeaderEntries", []
        )
        l = []
        for row in report.get("data", {}).get("rows", []):
            dimensions = row.get("dimensions", [])
            date_range_values = row.get("metrics", [])
            d = {}
            for i, values in enumerate(date_range_values):
                d["batch_import_date"] = ds
                for header, dimension in zip(dimension_headers, dimensions):
                    d[header.replace("ga:", "")] = dimension
                for metricHeader, value in zip(metric_headers, values.get("values")):
                    d[metricHeader.get("name").replace("ga:", "")] = value
            l.append(d)
        logging.info(f"get {len(l)} lines")
        return l
    return None


with DAG(
    dag_id="historical_google_analytics_import",
    max_active_runs=1,
    concurrency=1,
    schedule_interval=None,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=True,
    start_date=pendulum.datetime(
        2021, 11, 4, tzinfo=pendulum.timezone("America/Toronto")
    ),
    on_failure_callback=slack_dag("slack_data_alerts"),
) as dag:
    end_date = "2021-10-19"
    DAYS = 5

    def build_deduplicate_query(dest_db: str, dest_schema: str, table: str) -> str:
        query = f"merge into {dest_db}.{dest_schema}.{table} using {dest_db}.{dest_schema}.historical_{table}_stage on "  # nosec
        for key in reports[table]["primary_keys"]:  # type: ignore
            query += f"{dest_db}.{dest_schema}.{table}.{key} = {dest_db}.{dest_schema}.historical_{table}_stage.{key} and "
        query = query[:-4]
        query += "when matched then delete"
        return query

    def process(table: str, conn: str, end_date: str, **context: Any) -> None:
        ds = context["ds"]
        start_date = (
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=(DAYS - 1))
        ).strftime("%Y-%m-%d")
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
                f"create or replace table {dest_db}.{dest_schema}.historical_{table}_stage as "  # nosec
                f"select $1 as fields from @{dest_schema}.{stage_guid}"  # nosec
            )
            if "primary_keys" in reports[table]:  # type: ignore
                tx.execute(build_deduplicate_query(dest_db, dest_schema, table))
            tx.execute(
                f"insert into {dest_db}.{dest_schema}.{table} "  # nosec
                f"select * from {dest_db}.{dest_schema}.historical_{table}_stage"  # nosec
            )
            tx.execute(f"drop table {dest_db}.{dest_schema}.historical_{table}_stage")

            logging.info(
                f"✔️ Successfully loaded historical {table} for {start_date} - {end_date} on {ds}"
            )

    while end_date >= "2021-07-05":
        dag << PythonOperator(
            task_id=f"import_acquisition_funnel_{end_date}",
            python_callable=process,
            op_kwargs={
                "conn": "snowflake_production",
                "table": "acquisition_funnel",
                "end_date": end_date,
            },
            provide_context=True,
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=DAYS)
        ).strftime("%Y-%m-%d")
