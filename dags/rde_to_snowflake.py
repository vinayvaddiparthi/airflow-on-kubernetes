import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.sql import Select

from utils.failure_callbacks import slack_dag


def ctas(catalog: str, schema: str, table: str) -> None:
    engine = create_engine("presto://presto-production-internal.presto.svc:8080/rde")

    with engine.begin() as tx:
        selectable = Select(
            [text("*")], from_obj=text(f'"rde"."risk-decision-engine"."{table}"')
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
        "rde_to_snowflake",
        start_date=pendulum.datetime(
            2019, 12, 17, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 9 * * *",
        catchup=False,
        description="",
        on_failure_callback=slack_dag("slack_data_alerts"),
    ) as dag:
        for schema, table in [
            ("public", "assignment_history"),
            ("public", "assignment_history_api"),
            ("public", "pricing_history"),
            ("public", "pricing_history_api"),
            ("public", "pricing_metadata"),
            ("public", "pricing_metadata_api"),
            ("public", "migrated_model_c"),
        ]:
            (
                dag
                << PythonOperator(
                    task_id=f"ctas__{schema}__{table}",
                    python_callable=ctas,
                    op_kwargs={"catalog": "sf_rde", "schema": schema, "table": table},
                    # on_failure_callback=slack_ti,
                )
                >> PythonOperator(
                    task_id=f"swap__{schema}__{table}",
                    python_callable=swap,
                    op_kwargs={
                        "conn": "snowflake_salesforce",
                        "database": "rde",
                        "schema": schema,
                        "table": table,
                    },
                    # on_failure_callback=slack_ti,
                )
            )

        return dag


globals()["rde_to_snowflake_dag"] = create_dag()
