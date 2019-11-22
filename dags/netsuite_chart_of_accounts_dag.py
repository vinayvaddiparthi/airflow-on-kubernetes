from datetime import datetime

import boto3
import pandas as pd
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 21),
    "retries": 0,
}

with DAG("netsuiteg_chart_of_account", schedule_interval=None, default_args=default_args) as dag:
    snowflake_hook = BaseHook.get_connection("snowflake_erp")
    aws_hook = AwsHook(aws_conn_id="s3_conn_id")
    aws_credentials = aws_hook.get_credentials()

    bucket = 'tc-datalake'
    prefix_account_internal_ids = 'erp_netsuite/chart_of_accounts/account_internal_ids/'
    prefix_chart_of_accounts = 'erp_netsuite/chart_of_accounts/chart_of_accounts/'


    def update_chart_of_accounts():
        data_type = {'TC (600)': str,
                     'Current GL Account ': str,
                     'CL': str,
                     'New Consolodiate GL Account Name': str,
                     'Entity': str}

        try:
            client = boto3.client(
                "s3",
                aws_access_key_id=aws_credentials.access_key,
                aws_secret_access_key=aws_credentials.secret_key,
            )
            objects = client.list_objects(Bucket=bucket, Prefix=prefix_chart_of_accounts, Delimiter="/")
            get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
            last_added = [file['Key'] for file in sorted(objects['Contents'], key=get_last_modified, reverse=True)][0]
            print(f"Loading file: {last_added}")
            obj = client.get_object(Bucket=bucket, Key=last_added)

            df = pd.read_csv(obj['Body'], delimiter=',', encoding="ISO-8859-1", dtype=data_type, skiprows=1)

            df = df[['TC (600)', 'Current GL Account ', 'CL', 'New Consolodiate GL Account Name', 'Entity']]
            df.columns = ['old_name', 'old_account', 'new_account', 'new_name', 'subsidiary']
            engine = create_engine(URL(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                role="sysadmin"
            ))
            conn = engine.connect()
            df.to_sql('chart_of_account', con=engine, if_exists='replace', index=False)
            conn.close()
            engine.dispose()
        except Exception as e:
            raise e


    def update_account_internal_ids():
        try:
            client = boto3.client(
                "s3",
                aws_access_key_id=aws_credentials.access_key,
                aws_secret_access_key=aws_credentials.secret_key,
            )
            objects = client.list_objects(Bucket=bucket, Prefix=prefix_account_internal_ids, Delimiter="/")
            get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
            last_added = [file['Key'] for file in sorted(objects['Contents'], key=get_last_modified, reverse=True)][0]
            print(f"Loading file: {last_added}")
            obj = client.get_object(Bucket=bucket, Key=last_added)
            df = pd.read_csv(obj['Body'], delimiter=',', encoding="ISO-8859-1", skiprows=5)
            df = df[['Internal ID', 'Number', 'Currency']]
            df.columns = ['internal_id', 'account', 'currency']
            engine = create_engine(URL(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                role="sysadmin"
            ))
            conn = engine.connect()
            df.to_sql('account_internal_ids', con=engine, if_exists='replace', index=False)
            conn.close()
            engine.dispose()
        except Exception as e:
            raise e


    dag << PythonOperator(
        task_id="update_chart_of_accounts",
        python_callable=update_chart_of_accounts
    ) >> PythonOperator(
        task_id="update_account_internal_ids",
        python_callable=update_account_internal_ids
    )
