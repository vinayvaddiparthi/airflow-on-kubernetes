import importlib

import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import datetime
from typing import Dict

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, text, column, func, TIMESTAMP, cast
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select


def ctas_to_glue(sfdc_instance: str, sobject: Dict):
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
        first_import: bool = tx.execute(
            f'CREATE TABLE IF NOT EXISTS "glue"."{sfdc_instance}"."{sobject_name}" AS {selectable}'
        ).fetchall()[0][0] >= 1

        if not first_import:
            try:
                max_date = tx.execute(
                    Select(
                        columns=[func.max(column(last_modified_field))],
                        from_obj=text(f'"glue"."{sfdc_instance}"."{sobject_name}"'),
                    )
                ).fetchall()[0][0]
                max_date = datetime.datetime.fromisoformat(max_date).__str__()
            except Exception:
                max_date = datetime.datetime.fromtimestamp(0).__str__()

            range_limited_selectable = selectable.where(
                column(last_modified_field) > cast(text(":max_date"), TIMESTAMP)
            )

            stmt = text(
                f'INSERT INTO "glue"."{sfdc_instance}"."{sobject_name}" {range_limited_selectable}'
            ).bindparams(max_date=max_date)

            tx.execute(stmt).fetchall()


def ctas_to_snowflake(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    selectable: Select = Select(
        [text("*")], from_obj=text(f'"{sfdc_instance}"."salesforce"."{sobject_name}"')
    )

    with engine.begin() as tx:
        first_import: bool = tx.execute(
            f'CREATE TABLE IF NOT EXISTS "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" AS {selectable}'
        ).fetchall()[0][0] >= 1

        if not first_import:
            try:
                max_date = tx.execute(
                    Select(
                        columns=[func.max(column(last_modified_field))],
                        from_obj=text(
                            f'"sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}"'
                        ),
                    )
                ).fetchall()[0][0]
                max_date = datetime.datetime.fromisoformat(max_date).__str__()
            except Exception:
                max_date = datetime.datetime.fromtimestamp(0).__str__()

            # cols_ = tx.execute(
            #     Select(
            #         [column("column_name"), column("data_type")],
            #         from_obj=text(f'"{sfdc_instance}"."salesforce"."columns"'),
            #     )
            #     .where(column("table_schema") == text(f"'{sfdc_instance}'"))
            #     .where(column("table_name") == text(f"'{sobject_name}'"))
            # ).fetchall()
            #
            # processed_columns = []
            # for name_, type_ in cols_:
            #     if type_ == "varchar":
            #         type_ = "varchar(16777216)"
            #
            #     processed_columns.append(
            #         text(f'CAST("{column(name_)}" AS {type_}) AS "{column(name_)}"')
            #     )

            selectable: Select = Select(
                [text("*")],
                from_obj=text(f'"{sfdc_instance}"."salesforce"."{sobject_name}"'),
            ).where(text(last_modified_field) > cast(text(":max_date"), TIMESTAMP))

            stmt = text(
                f'INSERT INTO "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" {selectable}'
            ).bindparams(max_date=max_date)

            tx.execute(stmt).fetchall()


def create_sf_summary_table(conn: str, sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine: Engine = SnowflakeHook(snowflake_conn_id=conn).get_sqlalchemy_engine()

    with engine.begin() as tx:
        tx.execute(
            f"""
        CREATE OR REPLACE TABLE "SALESFORCE"."{sfdc_instance.upper()}"."{sobject_name.upper()}"
        AS SELECT DISTINCT t0.* FROM "SALESFORCE"."{sfdc_instance.upper()}_RAW"."{sobject_name.upper()}" t0
        JOIN (
            SELECT id, max({last_modified_field}) AS max_date
            FROM "SALESFORCE"."{sfdc_instance.upper()}_RAW"."{sobject_name.upper()}"
            GROUP BY id
            ) t1 ON t0.id = t1.id AND t0.{last_modified_field} = t1.max_date
        """
        ).fetchall()


def create_dag(instance: str):
    sobjects = importlib.import_module(
        f"salesforce_import_extras.sobjects.{instance}"
    ).sobjects

    with DAG(
        f"{instance}_import",
        start_date=pendulum.datetime(
            2019, 10, 12, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="5 4 * * *",
        catchup=False,
    ) as dag:
        for sobject in sobjects:
            dag << PythonOperator(
                task_id=f'snowflake__{sobject["name"]}',
                python_callable=ctas_to_snowflake,
                op_kwargs={"sfdc_instance": instance, "sobject": sobject},
                pool="snowflake_pool",
            ) >> PythonOperator(
                task_id=f'snowflake_summary__{sobject["name"]}',
                python_callable=create_sf_summary_table,
                op_kwargs={
                    "conn": "snowflake_default",
                    "sfdc_instance": instance,
                    "sobject": sobject,
                },
                pool="snowflake_pool",
            )

    return dag


for instance in ["sfoi", "sfni"]:
    globals()[f"import_{instance}"] = create_dag(instance)
