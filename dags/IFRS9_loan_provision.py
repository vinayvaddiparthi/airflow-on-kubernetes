from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import timedelta

from sqlalchemy.sql import select
from sqlalchemy import Table, MetaData, engine

from utils.failure_callbacks import slack_dag, slack_task

import logging
import pendulum
import pandas as pd
import numpy as np


def read_data_from_snowflake(
    snowflake_engine: engine, database: str, schema: str
) -> pd.DataFrame:

    metadata = MetaData()

    loan = Table(
        "dim_loan",
        metadata,
        autoload_with=snowflake_engine,
        schema=schema,
        snowflake_database=database,
    )
    loan_select = select(
        [
            loan.c.id,
            loan.c.guid,
            loan.c.default_flag,
            loan.c.bankruptcy,
            loan.c.outstanding_balance,
            loan.c.calculated_dpd,
        ]
    )
    with snowflake_engine.begin() as conn:
        df = pd.read_sql(loan_select, conn)
        df = df.astype({"calculated_dpd": int, "outstanding_balance": float})
    return df


def provision_rate(row: pd.Series) -> float:
    if row["calculated_dpd"] in range(1, 15):
        rate = 0.4748
    elif row["calculated_dpd"] in range(16, 30):
        rate = 0.5952
    elif row["calculated_dpd"] in range(31, 90):
        rate = 0.7014
    elif row["calculated_dpd"] in range(91, 150):
        rate = 0.7114
    elif row["calculated_dpd"] in range(151, 180):
        rate = 0.8401
    elif row["calculated_dpd"] > 180:
        rate = 1
    else:
        rate = 0.0538
    return rate


def provision_loss_calculation(snowflake_conn: str, database: str, schema: str) -> None:
    snowflake_engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    df = read_data_from_snowflake(snowflake_engine, database, schema)
    logging.info("✅ Reading data from snowflake successful")
    df["calculated_dpd"] = np.where(
        np.logical_and(
            np.logical_or(df["default_flag"] == True, df["bankruptcy"] == True),
            df["calculated_dpd"] == 0,
        ),
        91,
        df["calculated_dpd"],
    )
    df["provision_rate"] = df.apply(lambda row: provision_rate(row), axis=1)
    df["provision_loss"] = df["outstanding_balance"] * df["provision_rate"]

    destination_schema = "dbt_reporting"
    stage = f"{database}.{destination_schema}.provision_loss"

    df.to_csv("IFRS9", index=False, header=False)

    logging.info("✅ IFRS9 provision loss report written to a csv file")

    with snowflake_engine.begin() as conn:

        try:
            conn.execute(f"create or replace stage {stage} file_format=(type=csv)")

            logging.info(f"✅ Created snowflake stage successfully: {stage}")

            conn.execute(f"put file://IFRS9 @{stage}")

            logging.info("✅ Copied the csv file to snowflake stage succesfully")

        except Exception as e:
            raise Exception(
                f"Provision Loss Report  not copied to snowflake stage. Exception details: {e} "
            )


def create_dag() -> DAG:
    is_prod = Variable.get(key="environment") == "production"

    with DAG(
        dag_id="IFRS9",
        description="This DAG will give IFRS9 Loan provision for credit loss report: when a customer does not repay the loan on time",
        start_date=pendulum.datetime(
            2022, 8, 4, tz=pendulum.timezone("America/Toronto")
        ),
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": slack_task("slack_data_alerts"),
        },
        schedule_interval="0 10 * * 5",
        catchup=False,
        max_active_runs=1,
        on_failure_callback=slack_dag("slack_data_alerts"),
    ) as dag, open("dags/sql/IFRS9_provision_loss.sql") as provision_loss_sql:

        read_data_from_snowflake = PythonOperator(
            task_id="read_data_fom_snowflake",
            python_callable=provision_loss_calculation,
            op_kwargs={
                "snowflake_conn": "snowflake_dbt",
                "database": f"{'analytics_production' if is_prod else 'analytics_development'}",
                "schema": "dbt_ario",
            },
            provide_context=False,
        )

        write_to_snowflake = SnowflakeOperator(
            task_id="write_to_snowflake",
            sql=[query.strip("\n") for query in provision_loss_sql.read().split(";")],
            params={
                "stage": "provision_loss",
                "table": "fct_provision_loss",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema="dbt_reporting",
            snowflake_conn_id="snowflake_dbt",
            dag=dag,
        )

        read_data_from_snowflake >> write_to_snowflake

    return dag


dag = create_dag()
