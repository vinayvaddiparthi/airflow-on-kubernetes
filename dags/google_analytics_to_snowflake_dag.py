import json
from datetime import datetime

import pandas as pd
import pendulum
import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from utils.failure_callbacks import slack_on_fail

# view_id from GA: Overall - IP and spam filtered
VIEW_ID = "102945619"
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
    "source_medium_with_gid": {
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
                "dimensions": [{"name": "ga:sourceMedium"}, {"name": "ga:dimension5"}],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
    "google_ads_campaigns": {
        "reportRequests": [
            {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": None, "endDate": None}],
                "metrics": [
                    # acquisition
                    {"expression": "ga:adClicks"},
                    {"expression": "ga:adCost"},
                    {"expression": "ga:CPC"},
                    {"expression": "ga:users"},
                    {"expression": "ga:sessions"},
                    # behavior
                    {"expression": "ga:bounceRate"},
                    {"expression": "ga:pageviewsPerSession"},
                    # conversion
                    {"expression": "ga:goal1ConversionRate"},
                    {"expression": "ga:goal1Completions"},
                    {"expression": "ga:goal1Value"},
                ],
                "dimensions": [{"name": "ga:campaign"}],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
    "google_ads_campaigns_with_gid": {
        "reportRequests": [
            {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": None, "endDate": None}],
                "metrics": [
                    # acquisition
                    {"expression": "ga:adClicks"},
                    {"expression": "ga:adCost"},
                    {"expression": "ga:CPC"},
                    {"expression": "ga:users"},
                    {"expression": "ga:sessions"},
                    # behavior
                    {"expression": "ga:bounceRate"},
                    {"expression": "ga:pageviewsPerSession"},
                    # conversion
                    {"expression": "ga:goal1ConversionRate"},
                    {"expression": "ga:goal1Completions"},
                    {"expression": "ga:goal1Value"},
                ],
                "dimensions": [{"name": "ga:campaign"}, {"name": "ga:dimension5"}],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
    "channels": {
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
                "dimensions": [{"name": "ga:channelGrouping"}],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
    "channels_with_gid": {
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
                "dimensions": [
                    {"name": "ga:channelGrouping"},
                    {"name": "ga:dimension5"},
                ],
                "pageToken": "0",
                "pageSize": ROW_LIMIT,
            }
        ]
    },
}

with DAG(
    "google_analytics_to_snowflake_dag",
    schedule_interval="@daily",
    start_date=pendulum.datetime(
        2020, 1, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:

    def initialize_analytics_reporting():
        hook = GoogleCloudBaseHook(gcp_conn_id="google_analytics_default")

        key = json.loads(hook._get_field("keyfile_dict"))
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            key, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        # Build the service object with google api's build function
        analytics = build("analyticsreporting", "v4", credentials=credentials)

        return analytics

    def get_report(analytics, table, ds, page_token):
        print(f"Current page_token: {page_token}")
        payload = reports[table]
        payload["reportRequests"][0]["dateRanges"][0]["startDate"] = ds
        payload["reportRequests"][0]["dateRanges"][0]["endDate"] = ds
        if page_token is not None:
            print(f"Overwrite page_token:{page_token}")
            payload["reportRequests"][0]["pageToken"] = page_token
        return analytics.reports().batchGet(body=payload).execute()

    def transform_response_to_df(response, ds):
        page_token = None
        if "nextPageToken" in response.get("reports", [])[0]:
            page_token = response.get("reports", [])[0]["nextPageToken"]
            print(f"nextPageToken:{page_token}")
        # getting the first report
        for report in response.get("reports", []):
            columnHeader = report.get("columnHeader", {})
            dimensionHeaders = columnHeader.get("dimensions", [])
            metricHeaders = columnHeader.get("metricHeader", {}).get(
                "metricHeaderEntries", []
            )
            list = []
            for row in report.get("data", {}).get("rows", []):
                dimensions = row.get("dimensions", [])
                dateRangeValues = row.get("metrics", [])
                dict = {}
                for i, values in enumerate(dateRangeValues):
                    dict["date"] = pd.to_datetime(ds, format="%Y-%m-%d")
                    for header, dimension in zip(dimensionHeaders, dimensions):
                        dict[header.replace("ga:", "")] = dimension
                    for metricHeader, value in zip(metricHeaders, values.get("values")):
                        dict[metricHeader.get("name").replace("ga:", "")] = value
                list.append(dict)
            df = pd.DataFrame(list)
            return df, page_token

    def df_to_sql(conn: str, schema: str, table: str, **context):
        ds = context["ds"]
        print(f"Date Range:{ds}")
        analytics = initialize_analytics_reporting()
        page_token = "0"
        while page_token is not None:
            response = get_report(analytics, table, ds, page_token)
            df, token = transform_response_to_df(response, ds)
            print(df.head(3))
            df["date"] = df["date"].dt.date
            snowflake_hook = BaseHook.get_connection(conn)

            with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="etl",
                database="google_analytics",
                schema=schema,
                ocsp_fail_open=False,
            ) as conn:
                engine = create_engine(
                    f"snowflake://{snowflake_hook.host}.snowflakecomputing.com",
                    creator=lambda: conn,
                )
                if response:
                    df.to_sql(
                        table, engine, if_exists="append", method="multi", index=False
                    )
                    print(f"Row count: {len(response)}")
            if token is not None:
                page_token = str(token)
            else:
                page_token = None
            print(f"Row count: {len(df.values)} loaded")

    for report in reports:
        dag << PythonOperator(
            task_id=f"task_{report}",
            python_callable=df_to_sql,
            op_kwargs={
                "conn": "snowflake_default",
                "schema": "public",
                "table": f"{report}",
            },
            provide_context=True,
            pool="ga_pool",
            on_failure_callback=slack_on_fail,
        )
