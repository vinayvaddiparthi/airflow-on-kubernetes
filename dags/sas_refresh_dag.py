import pendulum
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template
import snowflake.connector

tables = [
    "ACCOUNT_OWNER",
    "ACCOUNT_STATUS",
    "CONTRACT_HISTORY",
    "DAYS_NON_PAYMENT",
    "DAYS_PAST_DUE",
    "LOAN_ACCOUNT",
    "LOAN_ACCOUNT_SUMMARY",
    "PRINCIPAL_BALANCE",
    "TLI_CATEGORIZED",
    "TOTAL_BALANCE",
    "EQB_ACCOUNT_STATUS",
    "EQB_ELIGIBILITY_STATUS",
    "EQB_PURCHASE_STATUS",
]

with DAG(
    "sas_refresh_dag",
    schedule_interval="30 12 * * *",
    start_date=pendulum.datetime(
        2020, 1, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    snowflake_hook = BaseHook.get_connection("snowflake_sas")

    def grant_access():
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="SAS",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn, open("dags/sas_refresh_extras/grant_select_on_sas.sql") as f:
            sql_template = Template(f.read())

            cur = conn.cursor()

            for role in ["analyst_role", "looker_role"]:
                sql = sql_template.render(role=role)
                cur.execute(sql)
            conn.commit()

    def create_table():
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="SAS",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn, open("dags/sas_refresh_extras/sas_refresh.sql") as f:
            sql_template = Template(f.read())

            cur = conn.cursor()

            for table in tables:
                sql = sql_template.render(table=table)
                cur.execute(sql)

            conn.commit()

    task_grant_access = PythonOperator(
        task_id="task_grant_access", python_callable=grant_access, dag=dag
    )

    task_create_table = PythonOperator(
        task_id="task_create_table", python_callable=create_table, dag=dag
    )

    task_create_table >> task_grant_access
