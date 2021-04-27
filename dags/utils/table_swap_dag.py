from typing import List, Dict

import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.sql import Select


def ctas(input_catalog: str, output_catalog: str, src: Dict, dst: Dict) -> None:
    output_catalog = output_catalog.upper()

    engine = create_engine("presto://presto-production-internal.presto.svc:8080")

    with engine.begin() as tx:
        selectable = Select(
            [text("*")],
            from_obj=text(f'"{input_catalog}"."{src["schema"]}"."{src["table"]}"'),
        )

        stmt = f'CREATE TABLE IF NOT EXISTS "{output_catalog}"."{dst["schema"]}"."{dst["table"]}__swap" AS {selectable}'
        tx.execute(stmt).fetchall()


def swap(conn: str, output_database: str, dst: Dict) -> None:
    output_database = output_database.upper()
    schema = dst["schema"].upper()
    table = dst["table"].upper()

    with SnowflakeHook(conn).get_sqlalchemy_engine().begin() as tx:
        tx.execute(f'DROP TABLE IF EXISTS "{output_database}"."{schema}"."{table}"')
        tx.execute(
            f'ALTER TABLE IF EXISTS "{output_database}"."{schema}"."{table}__SWAP" RENAME TO "{output_database}"."{schema}"."{table}"'
        )


def create_table_swap_dag(
    dag_name: str,
    start_date: pendulum.datetime,
    input_catalog: str,
    output_catalog: str,
    output_database: str,
    tables: List[Dict],
) -> DAG:
    with DAG(
        dag_name,
        start_date=start_date,
        schedule_interval="0 9 * * *",
        catchup=False,
        description="",
    ) as dag:
        for table in tables:
            (
                dag
                << PythonOperator(
                    task_id=f'ctas__{table["src"]["schema"]}__{table["dst"]["table"]}',
                    python_callable=ctas,
                    op_kwargs={
                        "input_catalog": input_catalog,
                        "output_catalog": output_catalog,
                        "src": table["src"],
                        "dst": table["dst"],
                    },
                )
                >> PythonOperator(
                    task_id=f'swap__{table["src"]["schema"]}__{table["src"]["table"]}',
                    python_callable=swap,
                    op_kwargs={
                        "conn": "airflow_production",
                        "output_database": output_database,
                        "dst": table["dst"],
                    },
                )
            )

    return dag
