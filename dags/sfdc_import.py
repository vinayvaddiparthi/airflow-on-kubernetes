import importlib
import json

import pendulum

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import datetime
from typing import Dict

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, text, column, func, TIMESTAMP, cast
from sqlalchemy.sql import Select

from utils.failure_callbacks import slack_on_fail


def ctas_to_snowflake(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    try:
        selectable: Select = sobject["selectable"]["callable"](
            table=sobject_name,
            engine=engine,
            **(sobject["selectable"].get("kwargs", {})),
        )
    except KeyError:
        selectable: Select = Select(
            columns=[text("*")],
            from_obj=text(f'"{sfdc_instance}"."salesforce"."{sobject_name}"'),
        )

    with engine.begin() as tx:
        cols_ = tx.execute(
            Select(
                [column("column_name"), column("data_type")],
                from_obj=text(f'"{sfdc_instance}"."information_schema"."columns"'),
            )
            .where(column("table_schema") == text(f"'salesforce'"))
            .where(column("table_name") == text(f"'{sobject_name}'"))
        ).fetchall()

        processed_columns = []
        for name_, type_ in cols_:
            if type_ == "varchar":
                type_ = "varchar(16777216)"

            processed_columns.append(
                text(f'CAST("{column(name_)}" AS {type_}) AS "{column(name_)}"')
            )

        selectable = Select(processed_columns, from_obj=selectable)

        first_import: bool = tx.execute(
            f'CREATE TABLE IF NOT EXISTS "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" AS {selectable}'
        ).fetchone()[0] >= 1

        if not first_import:
            with SnowflakeHook(
                "snowflake_salesforce"
            ).get_sqlalchemy_engine().begin() as sf_tx:
                max_date = sf_tx.execute(
                    Select(
                        columns=[func.max(column(last_modified_field))],
                        from_obj=text(
                            f'"salesforce"."{sfdc_instance}_raw"."{sobject_name}"'.upper()
                        ),
                    )
                ).fetchone()[0] or datetime.datetime.fromtimestamp(0)

            selectable = selectable.where(
                text(last_modified_field) > cast(text(f"'{max_date}'"), TIMESTAMP)
            )

            tx.execute(
                f'INSERT INTO "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" {selectable}'
            ).fetchall()


def create_sf_summary_table(conn: str, sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    from_obj = text(f'"salesforce"."{sfdc_instance}_raw"."{sobject_name}"'.upper())
    to_obj = text(f'"salesforce"."{sfdc_instance}"."{sobject_name}"'.upper())

    with SnowflakeHook(snowflake_conn_id=conn).get_sqlalchemy_engine().begin() as tx:
        selectable = Select(
            columns=[
                text("*"),
                func.row_number()
                .over(
                    partition_by=column("id"),
                    order_by=column(last_modified_field).desc(),
                )
                .label("_RN"),
            ],
            from_obj=from_obj,
        )

        tx.execute(
            f"CREATE OR REPLACE TRANSIENT TABLE {to_obj} AS {selectable} QUALIFY _RN = 1"
        ).fetchall()


def create_dag(instance: str):
    sobjects = importlib.import_module(
        f"salesforce_import_extras.sobjects.{instance}"
    ).sobjects

    dbt_hook = BaseHook.get_connection("dbt_refresh")
    gl_user = json.loads(dbt_hook.extra)["user"]
    gl_token = json.loads(dbt_hook.extra)["token"]

    with DAG(
        f"{instance}_import",
        start_date=pendulum.datetime(
            2019, 10, 12, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 0,18 * * *",
        catchup=False,
    ) as dag:
        for sobject in sobjects:
            dag << PythonOperator(
                task_id=f'snowflake__{sobject["name"]}',
                python_callable=ctas_to_snowflake,
                op_kwargs={"sfdc_instance": instance, "sobject": sobject},
                pool=f"{instance}_pool",
                execution_timeout=datetime.timedelta(hours=4),
                retry_delay=datetime.timedelta(hours=1),
                retries=3,
                on_failure_callback=slack_on_fail,
                priority_weight=sobject.get("weight", 0),
            ) >> PythonOperator(
                task_id=f'snowflake_summary__{sobject["name"]}',
                python_callable=create_sf_summary_table,
                op_kwargs={
                    "conn": "snowflake_salesforce",
                    "sfdc_instance": instance,
                    "sobject": sobject,
                },
                pool="snowflake_pool",
                execution_timeout=datetime.timedelta(hours=4),
                retry_delay=datetime.timedelta(hours=1),
                retries=3,
                priority_weight=sobject.get("weight", 0),
            ) >> BashOperator(
                task_id="dbt_refresh",
                pool="snowflake_pool",
                execution_timeout=datetime.timedelta(hours=4),
                retry_delay=datetime.timedelta(hours=1),
                retries=3,
                priority_weight=sobject.get("weight", 0),
                bash_command="git clone https://${gl_user}:${gl_token}@gitlab.com/tc-data/curated-data-warehouse.git"
                "&& cd curated-data-warehouse"
                "&& dbt seed --profiles-dir . --target prod"
                "&& dbt run --profiles-dir . --target prod"
                "&& dbt snapshot --profiles-dir . --target prod",
                env={
                    "SNOWFLAKE_ACCOUNT": dbt_hook.host,
                    "SNOWFLAKE_USERNAME": dbt_hook.login,
                    "SNOWFLAKE_PASSWORD": dbt_hook.password,
                    "SNOWFLAKE_DATABASE": "ANALYTICS_PRODUCTION",
                    "SNOWFLAKE_SCHEMA": "DBT_ARIO",
                    "SNOWFLAKE_ROLE": "DBT_PRODUCTION",
                    "SNOWFLAKE_WAREHOUSE": "ETL",
                    "ZETATANGO_ENV": "PRODUCTION",
                    "gl_user": gl_user,
                    "gl_token": gl_token,
                },
            )
        return dag


for instance in ["sfoi", "sfni"]:
    globals()[f"import_{instance}"] = create_dag(instance)
