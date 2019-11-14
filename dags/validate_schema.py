import importlib
import logging
from typing import List, Dict

import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine


def validate_schema(sfdc_instance: str, sobjects: List[Dict]):
    for sobject in sobjects:
        engine = create_engine(f"presto://presto-production-internal.presto.svc:8080")

        with engine.begin() as tx:

            sfdc_columns = tx.execute(
                f'SELECT "column_name", "data_type" FROM "{sfdc_instance}"."information_schema"."columns" '
                f"WHERE \"table_name\"='{sobject['name']}'"
            ).fetchall()

            snow_columns = tx.execute(
                f'SELECT "column_name", "data_type" FROM "sf_salesforce"."information_schema"."columns" '
                f"WHERE \"table_name\"='{sobject['name']}' AND \"table_schema\"='{sfdc_instance}_raw'"
            ).fetchall()

            # Pretend that unbounded varchars are varchar(16777216) for schema comparison purposes,
            # since snowflake does not support this data type. During the loading process these fields
            # will be casted accordingly.
            snow_columns = [
                (
                    column_name,
                    "varchar(16777216)" if data_type == "varchar" else data_type,
                )
                for column_name, data_type in snow_columns
            ]

            sfdc_set = {tuple(x) for x in sfdc_columns}
            snow_set = {tuple(x) for x in snow_columns}

            tables_in_snow_not_in_sfdc = snow_set - sfdc_set
            if len(tables_in_snow_not_in_sfdc) > 0:
                logging.warning(
                    f"Found columns in Snowflake which couldn't be matched to SFDC ({sfdc_instance}): {tables_in_snow_not_in_sfdc}"
                )

            tables_in_sfdc_not_in_snow = sfdc_set - snow_set
            if len(tables_in_sfdc_not_in_snow) > 0:
                logging.warning(
                    f"Found columns in SFDC which couldn't be matched to Snowflake ({sfdc_instance}): {tables_in_sfdc_not_in_snow}"
                )


def create_dag(instance: str):
    sobjects = importlib.import_module(
        f"salesforce_import_extras.sobjects.{instance}"
    ).sobjects

    with DAG(
        f"validate_{instance}",
        start_date=pendulum.datetime(
            2019, 11, 12, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=None,
        catchup=False,
    ) as dag:
        schema_validation_job = PythonOperator(
            task_id="schema_validation",
            python_callable=validate_schema,
            op_kwargs={"sfdc_instance": instance, "sobjects": sobjects},
        )

    return dag


# globals()["validate_sfni"] = create_dag("sfni")
