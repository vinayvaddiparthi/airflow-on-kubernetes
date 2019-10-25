from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 21),
    "retries": 0,
}

with DAG(
    "sas_to_snowflake_dag", schedule_interval="0 9 * * *", default_args=default_args
) as dag:
    sobjects = [
        "account_owner",
        "account_status",
        "contract_history",
        "days_non_payment",
        "days_past_due",
        "loan_account",
        "loan_account_summary",
        "principal_balance",
        "total_balance",
    ]

    snowflake_hook = BaseHook.get_connection("snowflake_sas")
    aws_hook = AwsHook(aws_conn_id="s3_conn_id")
    aws_credentials = aws_hook.get_credentials()

    def copy_to_snowflake(name, **kwargs):
        sql_copy = f"""COPY INTO {name}_new 
                                FROM S3://tc-datalake/sas/external_tables/{name}/{name}.txt 
                                CREDENTIALS = (
                                    aws_key_id='{aws_credentials.access_key}',
                                    aws_secret_key='{aws_credentials.secret_key}')
                                    FILE_FORMAT=(field_delimiter='|',  FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""

        con = snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="SAS",
            schema="public",
            ocsp_fail_open=False,
        )

        with con.cursor() as cs:
            print(f"Copy to {name}_new table")
            cs.execute(f"drop table if exists {name}_new")
            cs.execute(f"create table {name}_new like {name}")
            cs.execute(sql_copy)

    def copy_tli_to_snowflake(part, **kwargs):
        sql_copy = f"""COPY INTO tli_categorized_new
        FROM S3://tc-datalake/sas/external_tables/tli/tli_categorized{part}.txt
        CREDENTIALS = (
        aws_key_id='{aws_credentials.access_key}',
        aws_secret_key='{aws_credentials.secret_key}')
        FILE_FORMAT=(field_delimiter='|', FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""

        con = snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="SAS",
            schema="public",
            ocsp_fail_open=False,
        )

        with con.cursor() as cs:
            print(f"Copy to tli_categorized_new table, part; {part}")
            cs.execute(
                f"create table if not exists tli_categorized_new like tli_categorized"
            )
            cs.execute(sql_copy)

    def check_and_swap(name, **kwargs):
        con = snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="SAS",
            schema="public",
            ocsp_fail_open=False,
        )

        with con.cursor() as cs:
            print(f"Check {name} tables")

            cs.execute(f"select count(*) from {name}_new")
            count_new = cs.fetchone()[0]
            cs.execute(f"select count(*) from {name}")
            count_old = cs.fetchone()[0]
            if count_new >= count_old * 0.9:
                cs.execute(f"drop table if exists {name}_old")
                cs.execute(f"create table {name}_old as select * from {name}")
                cs.execute(f"drop table if exists {name}")
                cs.execute(f"create table {name} as select * from {name}_new")
                cs.execute(f"drop table if exists {name}_new")
                print(f"Record count change [{name}]: {count_old} -> {count_new}")
            else:
                print(f"Record drop alert on [{name}]: {count_old} -> {count_new}")
                raise

    task_tli_1 = PythonOperator(
        task_id=f"snowflake_copy_tli_1",
        python_callable=copy_tli_to_snowflake,
        op_kwargs={"part": 1},
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    task_tli_2 = PythonOperator(
        task_id=f"snowflake_copy_tli_2",
        python_callable=copy_tli_to_snowflake,
        op_kwargs={"part": 2},
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    dag << task_tli_1 >> task_tli_2 >> PythonOperator(
        task_id=f"snowflake_check_and_swap_tli_categorized",
        python_callable=check_and_swap,
        op_kwargs={"name": "tli_categorized"},
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    for sobject in sobjects:
        dag << PythonOperator(
            task_id=f"snowflake_copy_{sobject}",
            python_callable=copy_to_snowflake,
            op_kwargs={"name": sobject},
            provide_context=True,
            trigger_rule=TriggerRule.NONE_FAILED,
        ) >> PythonOperator(
            task_id=f"snowflake_check_and_swap_{sobject}",
            python_callable=check_and_swap,
            op_kwargs={"name": sobject},
            provide_context=True,
            trigger_rule=TriggerRule.NONE_FAILED,
        )
