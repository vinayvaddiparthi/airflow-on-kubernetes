from datetime import datetime, timedelta

import boto3
import pandas as pd
import pendulum
import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator


# To solve Jan case for each year
def get_month_tag(date):
    last_month = date.month - 1 if date.month > 1 else 12
    last_year = date.year - 1 if last_month == 12 else date.year
    return f"{last_year}{str(last_month).zfill(2)}"


now = datetime.now()
month_tag = get_month_tag(now)
time_tag = now.strftime("%Y%m%d%H%M%S")

bucket = "tc-datalake"
prefix = "equifax_offline_batch/consumer/input"
input_file_name = f"tc_consumer_batch_{month_tag}.txt"

with DAG(
    "equifax_consumer_input_dag",
    schedule_interval="@monthly",
    start_date=pendulum.datetime(
        2020, 2, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    snowflake_hook = BaseHook.get_connection("snowflake_sas")
    aws_hook = AwsHook(aws_conn_id="s3_conn_id")
    aws_credentials = aws_hook.get_credentials()

    def create_request_file():
        header = f"BHDR-EQUIFAX{time_tag}ADVITFINSCOREDA2"
        trailer = f"BTRL-EQUIFAX{time_tag}ADVITFINSCOREDA2"

        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="etl",
            database="equifax",
            schema="input",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(
                f"""select customer_reference_number,
                           last_name,
                           first_name,
                           middle_name,
                           suffix,
                           filler,
                           sin,
                           dob,
                           address,
                           city,
                           province,
                           postal_code,
                           account_number,
                           filler2,
                           filler3
                    from equifax.input.consumer_{month_tag}"""
            )
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.to_csv(r"temp_formatted.csv", header=None, index=None, sep="\t")
            with open(
                "temp_formatted.csv", mode="r", encoding="utf-8"
            ) as file_in, open(
                "request_formatted.txt", mode="w", encoding="utf-8"
            ) as file_out:
                text = file_in.read()
                text = text.replace("\t", "")
                file_out.writelines(header)
                file_out.write("\n")
                file_out.write(text)
                file_out.writelines(trailer)
                file_out.writelines(str(len(df)).zfill(8))
            with open("request_formatted.txt", "rb") as file:
                client = boto3.client(
                    "s3",
                    aws_access_key_id=f"{aws_credentials.access_key}",
                    aws_secret_access_key=f"{aws_credentials.secret_key}",
                )
                client.upload_fileobj(file, bucket, f"{prefix}/{input_file_name}")
            with open("request_formatted.txt", mode="r") as file:
                lines = file.readlines()
                c = 0
                for line in lines:
                    if len(line) != 221:
                        print(f"Length warning: {line}")
                        c += 1
                print(f"Wrong lines: {c}")

    def create_month_table():
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="etl",
            database="equifax",
            schema="input",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(
                f"create or replace table equifax.input.consumer_{month_tag} as select * from equifax.input.consumer"
            )
            cur.execute(f"select * from equifax.input.consumer_{month_tag}")
            data = cur.fetchall()
            df = pd.DataFrame(data)
        return

    def create_history_copy():
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="etl",
            database="equifax",
            schema="input",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(
                f"create or replace table equifax.input_history.consumer_{month_tag}_{time_tag} as select * from equifax.input.consumer_{month_tag}"
            )
        return

    dag << PythonOperator(
        task_id=f"create_month_table", python_callable=create_month_table
    ) >> PythonOperator(
        task_id=f"create_history_copy", python_callable=create_history_copy
    ) >> PythonOperator(
        task_id=f"create_request_file", python_callable=create_request_file
    )
