import pendulum
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.sql import Select
from utils.failure_callbacks import slack_dag


def ctas(catalog: str, schema: str, table: str) -> None:
    engine = create_engine(
        "presto://presto-production-internal.presto.svc:8080/sf_production_appv3"
    )

    with engine.begin() as tx:
        selectable = Select(
            [text("*")], from_obj=text(f'"production_appv3"."{schema}"."{table}"')
        )

        stmt = f'CREATE TABLE IF NOT EXISTS "{catalog}"."{schema}"."{table}__swap" AS {selectable}'
        tx.execute(stmt).fetchall()


def swap(conn: str, database: str, schema: str, table: str) -> None:
    database = database.upper()
    schema = schema.upper()
    table = table.upper()

    with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
        tx.execute(f'DROP TABLE IF EXISTS "{database}"."{schema}"."{table}"')
        tx.execute(
            f'ALTER TABLE IF EXISTS "{database}"."{schema}"."{table}__SWAP" RENAME TO "{database}"."{schema}"."{table}"'
        )


def create_dag() -> DAG:
    with DAG(
        "production_appv3_to_snowflake",
        start_date=pendulum.datetime(
            2019, 12, 17, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 9 * * *",
        catchup=False,
        description="",
        on_failure_callback=slack_dag("slack_data_alerts"),
    ) as dag:
        for schema, table in [
            ("tc_salesvolume", "currency_code"),
            ("tc_salesvolume", "data_quality"),
            ("tc_salesvolume", "ext_txn"),
            ("tc_salesvolume", "fin_account"),
            ("tc_salesvolume", "job_log"),
            ("tc_salesvolume", "sales_volume"),
            ("tc_salesvolume", "tc_txn_type"),
            ("tc_salesvolume", "txn_mode"),
            ("tc_salesvolume", "txn_reason_code"),
            ("tc_salesvolume", "txn_status"),
            ("tc_salesvolume", "txn_type"),
        ]:
            (
                dag
                << PythonOperator(
                    task_id=f"ctas__{schema}__{table}",
                    python_callable=ctas,
                    op_kwargs={
                        "catalog": "sf_production_appv3",
                        "schema": schema,
                        "table": table,
                    },
                    # on_failure_callback=slack_ti,
                )
                >> PythonOperator(
                    task_id=f"swap__{schema}__{table}",
                    python_callable=swap,
                    op_kwargs={
                        "conn": "snowflake_salesforce",
                        "database": "production_appv3",
                        "schema": schema,
                        "table": table,
                    },
                    # on_failure_callback=slack_ti,
                )
            )

        return dag


globals()["appv3_to_snowflake_dag"] = create_dag()
