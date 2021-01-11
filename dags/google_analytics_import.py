import json
import tempfile
from typing import Any, Dict
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build as AnalyticsBuild
from oauth2client.service_account import ServiceAccountCredentials

from utils import random_identifier
from utils.failure_callbacks import slack_ti

# view_id from GA: Overall - IP and spam filtered
VIEW_ID = "102376443"
ROW_LIMIT = 10000

reports: Dict[str, Any] = {
    "usr": {
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:sessions"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},
                        {"name": "ga:dimension5"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        }
    },
    "ad_cost": {
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:adClicks"},
                        {"expression": "ga:adCost"},
                    ],
                    "dimensions": [
                        {"name": "ga:date"},
                        {"name": "ga:adwordsCampaignID"},
                        {"name": "ga:campaign"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        }
    },
    "cx": {
        "primary_keys": [
            "fields:dimension6::string",
            "fields:eventAction::string",
            "fields:dateHourMinute::string",
        ],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:users"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},
                        {"name": "ga:hostname"},
                        {"name": "ga:pagePath"},
                        {"name": "ga:eventAction"},
                        {"name": "ga:date"},
                        {"name": "ga:dateHourMinute"},
                        {"name": "ga:eventCategory"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
    "acquisition": {
        "primary_keys": ["fields:dimension6::string", "fields:dateHourMinute::string"],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:newUsers"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},
                        {"name": "ga:sourceMedium"},
                        {"name": "ga:landingPagePath"},
                        {"name": "ga:fullReferrer"},
                        {"name": "ga:campaign"},
                        {"name": "ga:dateHourMinute"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
}


def initialize_analytics_reporting() -> Any:
    hook = GoogleCloudBaseHook(gcp_conn_id="google_analytics_default")
    key = json.loads(hook._get_field("keyfile_dict"))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        key, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    analytics = AnalyticsBuild(
        "analyticsreporting", "v4", credentials=credentials, cache_discovery=False
    )
    return analytics


def get_report(analytics: Any, table: str, ds: str, page_token: Any) -> Any:
    print(f"Current page_token: {page_token}")
    payload: Dict[str, Any] = reports[table]["payload"]
    payload["reportRequests"][0]["dateRanges"][0]["startDate"] = ds
    payload["reportRequests"][0]["dateRanges"][0]["endDate"] = ds
    if page_token:
        print(f"Overwrite page_token: {page_token}")
        payload["reportRequests"][0]["pageToken"] = page_token
    return analytics.reports().batchGet(body=payload).execute()


def next_page_token(response: Any) -> Any:
    page_token = None
    report_results = response.get("reports", [])
    if report_results:
        if "nextPageToken" in report_results[0]:
            page_token = report_results[0]["nextPageToken"]
            print(f"nextPageToken: {page_token}")
    return page_token


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
                d["date"] = ds
                for header, dimension in zip(dimension_headers, dimensions):
                    d[header.replace("ga:", "")] = dimension
                for metricHeader, value in zip(metric_headers, values.get("values")):
                    d[metricHeader.get("name").replace("ga:", "")] = value
            l.append(d)
        print(f"get {len(l)} lines")
        return l
    return None


with DAG(
    "google_analytics_import",
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    start_date=pendulum.datetime(
        2020, 8, 24, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:

    def build_deduplicate_query(dest_db: str, dest_schema: str, table: str) -> str:
        query = f"merge into {dest_db}.{dest_schema}.{table} using {dest_db}.{dest_schema}.{table}_stage on "  # nosec
        for key in reports[table]["primary_keys"]:  # type: ignore
            query += f"{dest_db}.{dest_schema}.{table}.{key} = {dest_db}.{dest_schema}.{table}_stage.{key} and "
        query = query[:-4]
        query += "when matched then delete"
        return query

    def process(table: str, conn: str, **context: Any) -> None:
        ds = context["ds"]
        print(f"Date Range: {ds}")
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
            print(
                f"create or replace temporary stage {dest_schema}.{stage_guid} "
                f"file_format=(type=csv)"
            )
            print("Initialize page_token")
            page_token: Any = "0"
            while page_token:
                response = get_report(analytics, table, ds, page_token)
                if response:
                    res_json = transform_raw_json(response, ds)
                    token = next_page_token(response)
                    with tempfile.TemporaryDirectory() as tempdir:
                        for i in range(len(res_json)):
                            json_filepath = Path(tempdir, f"{table}{i}").with_suffix(
                                ".json"
                            )
                            with open(json_filepath, "w") as outfile:
                                outfile.writelines(json.dumps(res_json[i]))
                            tx.execute(
                                f"put file://{json_filepath} @{dest_schema}.{stage_guid}"
                            ).fetchall()

                        # df.to_sql(
                        #     table, tx, if_exists="append", method="multi", index=False
                        # )
                    print(f"{table} row count: {len(response)}")
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
            print(f"✔️ Successfully grant access to tables in {dest_db}.{dest_schema}")
            print(f"✔️ Successfully loaded table {table} for {ds}")

    for report in reports:
        dag << PythonOperator(
            task_id=f"task_{report}",
            python_callable=process,
            op_kwargs={
                "conn": "airflow_production",
                "table": report,
            },
            provide_context=True,
            on_failure_callback=slack_ti("tc_failure_conn"),
        )
