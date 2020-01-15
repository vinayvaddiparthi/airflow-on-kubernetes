import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.sql import Select

from utils.failure_callbacks import slack_on_fail


def ctas(catalog: str, schema: str, table: str):
    engine = create_engine("presto://presto-production-internal.presto.svc:8080/sf_creditgenie")

    with engine.begin() as tx:
        selectable = Select(
            [text("*")], from_obj=text(f'"cg_lms_prod"."cg_lending"."{table}"')
        )

        stmt = f'CREATE TABLE IF NOT EXISTS "{catalog}"."{schema}"."{table}__swap" AS {selectable}'
        tx.execute(stmt).fetchall()


def swap(conn: str, database: str, schema: str, table: str):
    database = database.upper()
    schema = schema.upper()
    table = table.upper()

    with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
        tx.execute(f'DROP TABLE IF EXISTS "{database}"."{schema}"."{table}"')
        tx.execute(
            f'ALTER TABLE IF EXISTS "{database}"."{schema}"."{table}__SWAP" RENAME TO "{database}"."{schema}"."{table}"'
        )


def create_dag():
    with DAG(
        f"creditgenie_to_snowflake",
        start_date=pendulum.datetime(
            2020, 1, 15, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 9 * * *",
        catchup=False,
    ) as dag:
        for schema, table in [
            ("cg_lending", "bankaccount"),
            ("cg_lending", "customer"),
            ("cg_lending", "facility"),
            ("cg_lending", "financing"),
            ("cg_lending", "financingqueue"),
            ("cg_lending", "financingsetupaudit"),
            ("cg_lending", "financingstatistic"),
            ("cg_lending", "generalledger"),
            ("cg_lending", "institution"),
            ("cg_lending", "interestfrequency"),
            ("cg_lending", "manualtransactionqueue"),
            ("cg_lending", "manualtransactionqueuehistory"),
            ("cg_lending", "nsf"),
            ("cg_lending", "offlinerepayment"),
            ("cg_lending", "originatorfacility"),
            ("cg_lending", "partialrefundqueue"),
            ("cg_lending", "product"),
            ("cg_lending", "refund"),
            ("cg_lending", "repaymentfrequency"),
            ("cg_lending", "transaction"),
            ("cg_lending", "transactionbucket"),
            ("cg_lending", "transactioncategory"),
        ]:
            dag << PythonOperator(
                task_id=f"ctas__{schema}__{table}",
                python_callable=ctas,
                op_kwargs={"catalog": "sf_creditgenie", "schema": schema, "table": table},
                # on_failure_callback=slack_on_fail,
            ) >> PythonOperator(
                task_id=f"swap__{schema}__{table}",
                python_callable=swap,
                op_kwargs={
                    "conn": "snowflake_default",
                    "database": "creditgenie",
                    "schema": schema,
                    "table": table,
                },
                # on_failure_callback=slack_on_fail,
            )

        return dag


globals()["creditgenie_to_snowflake"] = create_dag()
