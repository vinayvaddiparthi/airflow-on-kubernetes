import pendulum
from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DagRun
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, column, text, VARCHAR, cast
from sqlalchemy.sql import Select


def generate_ctas(schema: str, table: str):
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

        stmt = f'CREATE TABLE "csportal_prod"."public"."{table}" AS {selectable}'
        tx.execute(stmt).fetchall()


with DAG(
    "feed_customer_portal",
    start_date=pendulum.datetime(
        2019, 10, 22, tzinfo=pendulum.timezone("America/Toronto")
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
            postgres_conn_id="postgres_csportal_prod",
            sql=f"DROP TABLE {table}",
        ) >> PythonOperator(
            task_id=f"ctas__{schema}__{table}",
            python_callable=generate_ctas,
            op_kwargs={"schema": schema, "table": table},
            on_failure_callback=lambda: SlackWebhookOperator(
                task_id="slack_fail",
                http_conn_id="slack_tc_data_channel",
                username="airflow",
                message=f"Task failed: ctas__{schema}__{table} in {{ dag.dag_id }} DagRun: {{ dag_run }}",
            ).execute(context=None),
        )
