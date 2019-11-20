from typing import Dict

import pendulum
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, column, text, VARCHAR, cast
from sqlalchemy.sql import Select

from utils.failure_callbacks import slack_on_fail


def generate_ctas(catalog: str, schema: str, table: str):
    engine = create_engine(
        "presto://presto-production-internal.presto.svc:8080/sf_csportal"
    )

    with engine.begin() as tx:
        cols_ = tx.execute(
            Select(
                [column("column_name"), column("data_type")],
                from_obj=text('"information_schema"."columns"'),
            )
            .where(column("table_schema") == text(f"'{schema}'"))
            .where(column("table_name") == text(f"'{table}'"))
        ).fetchall()

        c = []
        for col_ in cols_:
            if col_[1] == "varchar(16777216)":
                c.append(cast(column(col_[0]), VARCHAR).label(col_[0]))
            else:
                c.append(column(col_[0]).label(col_[0]))

        selectable = Select(c, from_obj=text(f'"sf_csportal"."public"."{table}"'))

        stmt = f'CREATE TABLE "{catalog}"."public"."{table}" AS {selectable}'
        tx.execute(stmt).fetchall()


def create_dag(conn: str, catalog: str):
    with DAG(
        f"feed_sbportal_{conn}",
        start_date=pendulum.datetime(
            2019, 11, 18, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 9 * * *",
        catchup=False,
    ) as dag:
        for schema, table in [
            ("public", "customer_business_information"),
            ("public", "loan_summary"),
        ]:
            dag << PostgresOperator(
                task_id=f"drop__{table}",
                postgres_conn_id=conn,
                sql=f"DROP TABLE IF EXISTS {table}",
                on_failure_callback=slack_on_fail,
            ) >> PythonOperator(
                task_id=f"ctas__{schema}__{table}",
                python_callable=generate_ctas,
                op_kwargs={"catalog": catalog, "schema": schema, "table": table},
                on_failure_callback=slack_on_fail,
            )

    return dag


for target, catalog in [
    ("postgres_sbportal_production", "sbportal_production_postgres"),
]:
    globals()[f"{target}_dag"] = create_dag(target, catalog)
