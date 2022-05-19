"""
quick start documentation: https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
"""
import logging
import json
import tempfile
from typing import Any, Dict
from pathlib import Path
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build as AnalyticsBuild
from oauth2client.service_account import ServiceAccountCredentials

from utils import random_identifier
from utils.failure_callbacks import slack_task
from data.google_analytics import reports
from utils.common_utils import get_utc_timestamp


def initialize_analytics_reporting() -> Any:
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
        An authorized Analytics Reporting API V4 service object.
    """
    hook = GoogleCloudBaseHook(gcp_conn_id="google_analytics_default")
    key = json.loads(hook._get_field("keyfile_dict"))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        key, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    analytics = AnalyticsBuild(
        "analyticsreporting", "v4", credentials=credentials, cache_discovery=False
    )
    return analytics


def get_report(
    analytics: Any, table: str, start_date: str, end_date: str, page_token: Any
) -> Any:
    """Queries the Analytics Reporting API V4.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        table: A configuration for Google Analytics report.
        start_date: Start of the data interval as YYYY-MM-DD.
        end_date: End of the data interval as YYYY-MM-DD.
        page_token: Page token.
    Returns:
        The Analytics Reporting API V4 response.
    """
    logging.info(f"Current page_token: {page_token}")
    payload: Dict[str, Any] = reports[table]["payload"]
    payload["reportRequests"][0]["dateRanges"][0]["startDate"] = start_date
    payload["reportRequests"][0]["dateRanges"][0]["endDate"] = end_date
    if page_token:
        logging.info(f"Overwrite page_token: {page_token}")
        payload["reportRequests"][0]["pageToken"] = page_token
    return analytics.reports().batchGet(body=payload).execute()


def next_page_token(response: Any) -> Any:
    """Return the next page token.

    Args:
        response: The Analytics Reporting API V4 response.
    Returns:
        A string represents the page token or None if not exists.
    """
    page_token = None
    report_results = response.get("reports", [])
    if report_results:
        if "nextPageToken" in report_results[0]:
            page_token = report_results[0]["nextPageToken"]
            logging.info(f"nextPageToken: {page_token}")
    return page_token


def transform_raw_json(raw: Dict, ds: str, import_ts: str) -> Any:
    """Transforms raw response.

    Args:
        raw: The Analytics Reporting API V4 response.
        ds: Start of the data interval as YYYY-MM-DD.
        import_ts: The timestamp of the import.
    Returns:
        List of dictionaries containing transformed data.
    Reference: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#report
    """
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
                d["date"] = ds
                d["import_ts"] = import_ts
                for header, dimension in zip(dimension_headers, dimensions):
                    d[header.replace("ga:", "")] = dimension
                for metricHeader, value in zip(metric_headers, values.get("values")):
                    d[metricHeader.get("name").replace("ga:", "")] = value
            l.append(d)
        logging.info(f"got {len(l)} lines")
        return l
    return None


with DAG(
    dag_id="google_analytics_import",
    max_active_runs=1,
    schedule_interval="0 */2 * * *",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack_task("slack_data_alerts"),
    },
    catchup=True,
    start_date=pendulum.datetime(
        2020, 8, 24, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:

    def build_deduplicate_query(dest_db: str, dest_schema: str, table: str) -> str:
        """Delete records from destination table based on the values of primary keys in destination and staging table.

        Args:
            dest_db: Database name of the destination table that needs deduplicate.
            dest_schema: Schema name of the destination table that needs deduplicate.
            table: Destination table that needs deduplicate.
        Returns:
            A string represents the Snowflake statement.
        """
        query = f"merge into {dest_db}.{dest_schema}.{table} using {dest_db}.{dest_schema}.{table}_stage on "  # nosec
        for key in reports[table]["primary_keys"]:  # type: ignore
            query += f"{dest_db}.{dest_schema}.{table}.{key} = {dest_db}.{dest_schema}.{table}_stage.{key} and "
        query = query[:-4]
        query += "when matched then delete"
        return query

    def process(table: str, conn: str, **context: Any) -> None:
        ds = context["ds"]
        logging.info(f"Date Range: {ds}")
        utc_time_now = get_utc_timestamp()
        analytics = initialize_analytics_reporting()
        google_analytics_hook = BaseHook.get_connection("google_analytics_snowflake")
        dest_db = google_analytics_hook.extra_dejson.get("dest_db")
        dest_schema = (
            google_analytics_hook.extra_dejson.get("dest_schema")
            if Variable.get(key="environment") == "production"
            else "development"
        )

        with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
            stage_guid = random_identifier()
            tx.execute(f"use database {dest_db}")
            tx.execute(
                f"create or replace temporary stage {dest_schema}.{stage_guid} file_format=(type=json)"
            ).fetchall()
            logging.info(
                f"create or replace temporary stage {dest_schema}.{stage_guid} "
                f"file_format=(type=json)"
            )
            logging.info("Initialize page_token")
            page_token: Any = "0"
            while page_token:
                response = get_report(analytics, table, ds, ds, page_token)
                if response:
                    res_json = transform_raw_json(response, ds, utc_time_now)

                    if len(res_json):
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
                    else:
                        logging.info(f"{table} row count: {len(res_json)}")

                    token = next_page_token(response)
                if token:
                    page_token = str(token)
                else:
                    page_token = None

            # check for new report
            result = tx.execute(
                f"show tables like '{table}' in {dest_db}.{dest_schema}"  # nosec
            ).fetchall()
            if len(result) > 0:
                # merge into existing report
                tx.execute(
                    f"create or replace table {dest_db}.{dest_schema}.{table}_stage as "  # nosec
                    f"select $1 as fields from @{dest_schema}.{stage_guid}"  # nosec
                )
                if "primary_keys" in reports[table]:  # type: ignore
                    tx.execute(build_deduplicate_query(dest_db, dest_schema, table))
                tx.execute(
                    f"insert into {dest_db}.{dest_schema}.{table} "  # nosec
                    f"select * from {dest_db}.{dest_schema}.{table}_stage"  # nosec
                )
                tx.execute(f"drop table {dest_db}.{dest_schema}.{table}_stage")
            else:
                # create table for new report
                tx.execute(
                    f"create or replace table {dest_db}.{dest_schema}.{table} as "  # nosec
                    f"select $1 as fields from @{dest_schema}.{stage_guid}"  # nosec
                )
                tx.execute(
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {dest_db}.{dest_schema} TO ROLE DBT_DEVELOPMENT"
                )
                tx.execute(
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {dest_db}.{dest_schema} TO ROLE DBT_PRODUCTION"
                )
                logging.info(
                    f"✔️ Successfully grant access to tables in {dest_db}.{dest_schema}"
                )
            logging.info(f"✔️ Successfully loaded table {table} for {ds}")

    for report in reports:
        dag << PythonOperator(
            task_id=f"task_{report}",
            python_callable=process,
            op_kwargs={"conn": "snowflake_production", "table": report,},
            provide_context=True,
        )
