import json
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from utils.failure_callbacks import slack_on_fail

# view_id from GA: Overall - IP and spam filtered
VIEW_ID = "102376443"
ROW_LIMIT = 10000

reports = {
    "source_medium": {
        "reportRequests": [
            {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": None, "endDate": None}],
                "metrics": [
                    # acquisition
                    {"expression": "ga:users"},
                    {"expression": "ga:newUsers"},
                    {"expression": "ga:sessions"},
                    # behavior
                    {"expression": "ga:bounceRate"},
                    {"expression": "ga:pageviewsPerSession"},
                    {"expression": "ga:avgSessionDuration"},
                    # conversion
                    {"expression": "ga:goal1ConversionRate"},
                    {"expression": "ga:goal1Completions"},
                    {"expression": "ga:goal1Value"},
                ],
                "dimensions": [{"name": "ga:sourceMedium"}],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
}


def initialize_analytics_reporting() -> Any:
    hook = GoogleCloudBaseHook(gcp_conn_id="google_analytics_default")
    key = json.loads(hook._get_field("keyfile_dict"))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        key, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    analytics = build("analyticsreporting", "v4", credentials=credentials)
    return analytics


def get_report(analytics: Any, table: str, ds: str, page_token: Any) -> Any:
    print(f"Current page_token: {page_token}")
    payload: Dict = reports[table]
    payload["reportRequests"][0]["dateRanges"][0]["startDate"] = ds
    payload["reportRequests"][0]["dateRanges"][0]["endDate"] = ds
    if page_token:
        print(f"Overwrite page_token: {page_token}")
        payload["reportRequests"][0]["pageToken"] = page_token
    return analytics.reports().batchGet(body=payload).execute()


def transform_response_to_df(response: Any, ds: str) -> tuple:
    page_token = None
    report_results = response.get("reports", [])
    if report_results:
        if "nextPageToken" in report_results[0]:
            page_token = report_results[0]["nextPageToken"]
            print(f"nextPageToken: {page_token}")
        # getting the first report
        for report in report_results:
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
                    d["date"] = pd.to_datetime(ds, format="%Y-%m-%d")
                    for header, dimension in zip(dimension_headers, dimensions):
                        d[header.replace("ga:", "")] = dimension
                    for metricHeader, value in zip(
                        metric_headers, values.get("values")
                    ):
                        d[metricHeader.get("name").replace("ga:", "")] = value
                l.append(d)
            df = pd.DataFrame(l)
            return df, page_token
        return "check here", None
    else:
        return None, None


with DAG(
    "google_analytics_import",
    schedule_interval="@daily",
    start_date=pendulum.datetime(
        2020, 8, 29, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:

    def df_to_sql(conn: str, table: str, **context: Any) -> None:
        ds = context["ds"]
        print(f"Date Range: {ds}")
        analytics = initialize_analytics_reporting()
        page_token: Any = "0"
        while page_token:
            response = get_report(analytics, table, ds, page_token)
            if response:
                df, token = transform_response_to_df(response, ds)
                print(df.head(3))
                df["date"] = df["date"].dt.date

                with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
                    df.to_sql(
                        table, tx, if_exists="append", method="multi", index=False
                    )
                print(f"Row count: {len(response)}")
            if token:
                page_token = str(token)
            else:
                page_token = None
            print(f"Row count: {len(df.values)} loaded")

    for report in reports:
        dag << PythonOperator(
            task_id=f"task_{report}",
            python_callable=df_to_sql,
            op_kwargs={
                "conn": "airflow_production",
                "table": f"{report}",
            },
            provide_context=True,
            on_failure_callback=slack_on_fail,
        )
