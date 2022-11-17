import logging
import pendulum
import pandas as pd
import numpy as np
import csv
import tempfile
from datetime import timedelta, datetime
from typing import Tuple

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from sqlalchemy.sql import select
from sqlalchemy import Table, MetaData, literal_column, engine

from utils.failure_callbacks import slack_dag, slack_task


def read_data_from_snowflake(
    snowflake_engine: engine, database: str, schema: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetches the required data from Snowflake and converts them into pandas dataframes

    Args:
        snowflake_engine (engine): SQL Alchemy engine for connecting to Snowflake
        database (str): snowflake database
        schema (str): snowflake schema

    Returns:
        Tuple(pd.DataFrame, pd.DataFrame): pandas dataframes of dim_loan and dim_holidays tables
    """

    metadata = MetaData()

    loan = Table(
        "dim_loan",
        metadata,
        autoload_with=snowflake_engine,
        schema=schema,
        snowflake_database=database,
    )

    holidays = Table(
        "holidays",
        metadata,
        autoload_with=snowflake_engine,
        snowflake_database=database,
    )

    loan_select = (
        select(
            [
                loan.c.guid,
                loan.c.apr,
                loan.c.principal_amount,
                loan.c.repayment_amount,
                loan.c.total_repayments_amount,
                loan.c.interest_amount,
                loan.c.repayment_schedule,
                loan.c.state,
                loan.c.remaining_principal,
                loan.c.interest_balance,
                loan.c.outstanding_balance,
            ]
        )
        .where(loan.c.state == "repaying")
        .where(loan.c.repayment_type == "fixed")
    )

    holidays_select = select([holidays]).where(literal_column('"is_holiday"') == 1)

    with snowflake_engine.begin() as conn:

        df_loan = pd.read_sql(loan_select, conn)
        df_holidays = pd.read_sql(holidays_select, conn)

    df_loan = df_loan.astype({"repayment_amount": float, "interest_amount": float})

    df_holidays["date"] = pd.to_datetime(df_holidays["date"]).dt.date

    return df_loan, df_holidays


def _calculate_all_paydown_schedules(
    snowflake_conn: str, database: str, schema: str
) -> None:
    """Calculates the amortization schedule for loans in 'repaying' state and loads them to a Snowflake stage

    Args:
        snowflake_conn (str): Airflow connection to be used
        database (str): source and destination database in Snowflake
        schema (str): source schema in Snowflake

    """

    snowflake_engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()

    df_loan, df_holidays = read_data_from_snowflake(snowflake_engine, database, schema)

    # dictionary of holidays for faster lookup
    holiday_schedule = dict(zip(df_holidays.date, df_holidays.is_holiday))

    destination_schema = "dbt_reporting"
    stage = f"{database}.{destination_schema}.amortization_schedules"

    df_loan["number_of_pay_cycles"] = (
        df_loan["remaining_principal"] / df_loan["repayment_amount"]
    ).astype(int)

    repayment_schedule = df_loan["repayment_schedule"]

    df_loan["interval"] = pd.Series(
        np.select(
            [
                repayment_schedule == "daily",
                repayment_schedule == "weekly",
                repayment_schedule == "bi-weekly",
            ],
            [1, 7, 14],
            1,
        ),
        index=df_loan.index,
    )

    df_loan["interest_percent_per_cycle"] = df_loan["apr"] / 365.0 / df_loan["interval"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv") as csv_file:

        csv_writer = csv.writer(csv_file)

        for row in df_loan.itertuples():

            # Only looks at loans in 'repaying' state
            if row.principal_amount > 0:

                guid = row.guid
                number_of_pay_cycles = row.number_of_pay_cycles
                interval = row.interval
                interest_percent_per_cycle = row.interest_percent_per_cycle
                repayment_amount = row.repayment_amount
                remaining_principal = row.remaining_principal
                repayment_date = datetime.today().date()

                for _ in range(0, number_of_pay_cycles):
                    opening_balance = remaining_principal
                    repayment_date = repayment_date + timedelta(days=interval)
                    interest = opening_balance * interest_percent_per_cycle
                    remaining_principal = remaining_principal - (
                        repayment_amount - interest
                    )
                    closing_balance = remaining_principal
                    # Check the hash map to see if the date is a holiday or not
                    while repayment_date in holiday_schedule:
                        repayment_date = repayment_date + timedelta(days=1)

                    csv_writer.writerow(
                        [
                            guid,
                            repayment_date,
                            opening_balance,
                            repayment_amount,
                            interest,
                            remaining_principal,
                            closing_balance,
                        ]
                    )
            else:
                continue

        logging.info("✅ Amortization schedule written to a csv file")

        with snowflake_engine.begin() as conn:

            try:
                conn.execute(f"create or replace stage {stage} file_format=(type=csv)")

                logging.info(f"✅ Created snowflake stage successfully: {stage}")

                conn.execute(f"put file://{csv_file.name} @{stage}")

                logging.info("✅ Copied the csv file to snowflake stage succesfully")

            except Exception as e:
                raise Exception(
                    f"Amortization schedule not copied to snowflake stage. Exception details: {e} "
                )


def create_dag() -> DAG:

    with DAG(
        dag_id="paydown_schedule",
        start_date=pendulum.datetime(
            2022, 6, 10, tz=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="30 4 * * 0",
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": slack_task("slack_data_alerts"),
        },
        catchup=False,
        max_active_runs=1,
        on_failure_callback=slack_dag("slack_data_alerts"),
    ) as dag, open("dags/sql/paydown_schedule.sql") as paydown_sql:

        is_prod = Variable.get(key="environment") == "production"

        queries = [query.strip("\n") for query in paydown_sql.read().split(";")]

        amortization_schedules = PythonOperator(
            task_id="calculate_paydown_schedules",
            python_callable=_calculate_all_paydown_schedules,
            op_kwargs={
                "snowflake_conn": "snowflake_dbt",
                "database": f"{'analytics_production' if is_prod else 'analytics_development'}",
                "schema": "dbt_ario",
            },
        )

        load_to_snowflake = SnowflakeOperator(
            task_id="load_to_snowflake",
            sql=queries,
            params={
                "stage": "amortization_schedules",
                "table": "fct_amortization_schedules",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema="dbt_reporting",
            snowflake_conn_id="snowflake_dbt",
            dag=dag,
        )

        amortization_schedules >> load_to_snowflake

    return dag


globals()["paydown_schedule"] = create_dag()
