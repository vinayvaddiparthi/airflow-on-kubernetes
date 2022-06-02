# Python Code for Translation Task
# Author: Amit Chandna
# Overall objective: Output an amortization table for all ongoing loans in the database

# Import statements
import logging

# from airflow.operators.python_operator import PythonOperator
# from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import csv
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# to-do Need to collect the appropriate data to work with from ❄️ look @ dim_loan and holidays and repayment_amount


def read_data_from_snowflake() -> pd.core.frame.DataFrame:
    # URL for the dim loan data
    url_dim_loan = URL(
        user="XXX",
        password="XXX",
        account="thinkingcapital.ca-central-1.aws",
        warehouse="ETL",
        database="analytics_production",
        schema="dbt_ario",
        role="DBT_DEVELOPMENT",
    )
    engine_dim_loan = create_engine(url_dim_loan)
    connection_dim_loan = engine_dim_loan.connect()
    query_dim_loan = """select GUID,
       ACTIVATED_AT,
       APR,
       PRINCIPAL_AMOUNT,
       REPAYMENT_AMOUNT,
       TOTAL_REPAYMENTS_AMOUNT,
       INTEREST_AMOUNT,
       REPAYMENT_SCHEDULE,
       STATE,
       REMAINING_PRINCIPAL,
       INTEREST_BALANCE,
   OUTSTANDING_BALANCE
from analytics_production.dbt_ario.dim_loan
where state = 'repaying' """
    df_dim_loan = pd.read_sql(query_dim_loan, connection_dim_loan)

    # The connection for the holidays data
    url_holidays = URL(
        user="XXX",
        password="XXX",
        account="thinkingcapital.ca-central-1.aws",
        warehouse="ETL",
        database="ANALYTICS_REVIEW_UPDATE_SNA_UJQGCO",
        schema="dbt",
        role="DBT_DEVELOPMENT",
    )
    engine_holidays = create_engine(url_holidays)
    connection_holidays = engine_holidays.connect()
    query_holidays = """select * from analytics_review_update_sna_ujqgco.dbt.holidays where "is_holiday"=1"""
    df_holidays = pd.read_sql(query_holidays, connection_holidays)

    # Process the holiday table to create the holiday hash later
    df_holidays["date"] = pd.to_datetime(df_holidays["date"])
    for i, row in df_holidays.iterrows():
        df_holidays.at[i, "date"] = row["date"].date()

    return df_dim_loan, df_holidays


# Pretty sure that we need to connect to snowflake to pull whatever data is going to be used here for full automation
# Expected Steps for this function:

# 1.) Connect to snowflake
# 2.) Pull the table(s) of interest
# 3.) Load the tables into memory (if possible)
# 4.) Return the tables for use in the paydown calculation
# 5.) NB: This might need to be done in a chunkwise fashion as the data input may be too large
# logging.info(f"Successfully connected to ❄️ for data collection")


# This function just pulls in data from disk to a pandas df object and then processes with pandas for the table
# Can be adjusted for PyArrow compatability at a later point in time if needed.
# NB: This is not a permanent function - will be replaced by read_data_from_snowflake for production
# def read_temp_csv_data(filepath: str) -> pd.core.frame.DataFrame:
#     if "dim_loan" in filepath:
#         df = pd.read_csv(filepath)
#         # Pre-Processing for later in the pipe
#         fields_for_use = [
#             "GUID",
#             "ACTIVATED_AT",
#             "APR",
#             "PRINCIPAL_AMOUNT",
#             "REPAYMENT_AMOUNT",
#             "TOTAL_REPAYMENTS_AMOUNT",
#             "INTEREST_AMOUNT",
#             "REPAYMENT_SCHEDULE",
#             "STATE",
#             "REMAINING_PRINCIPAL",
#             "INTEREST_BALANCE",
#             "OUTSTANDING_BALANCE",
#         ]
#         df = df[fields_for_use]
#         df["ACTIVATED_AT"] = pd.to_datetime(df["ACTIVATED_AT"])
#         logging.info("✅ Processed the data")
#     elif "holiday" in filepath:
#         df = pd.read_csv(filepath)
#         df["date"] = pd.to_datetime(df["date"])
#         for i, row in df.iterrows():
#             df.at[i, "date"] = row["date"].date()
#         logging.info("✅ Processed the data")
#     else:
#         df = False
#         logging.info("❌ Could not process the data, many errors to follow...")
#     return df


def all_known_holidays(df_holidays) -> dict:
    df = df_holidays
    # Successfully makes a dictionary (hash map) of all holiday data provided in the read in csv (SQL statement)
    # Ensure that date lookup becomes O(1) instead of O(n^2)
    holiday_hash = dict(zip(df.date, df.is_holiday))
    logging.info("✅ Collected all the Holiday info and made a hash map ")
    return holiday_hash


# Schedule in days will return a datetime object to show how many days are in between different payment schedules
def schedule_in_days(frequency: str) -> timedelta:
    if frequency == "daily":
        return timedelta(days=1)
    elif frequency == "weekly":
        return timedelta(days=7)
    elif frequency == "bi-weekly":
        return timedelta(days=14)
    else:
        return timedelta(days=0)


def interval_float(frequency: str) -> float:
    if frequency == "daily":
        return float(1)
    elif frequency == "weekly":
        return float(7)
    elif frequency == "bi-weekly":
        return float(14)
    else:
        return float(0)


def write_data_to_csv(
    guid: str,
    repayment_date: datetime,
    beginning_balance: float,
    repayment_amount: float,
    interest: float,
    ending_balance: float,
    filepath: str,
) -> None:
    file = open(filepath, "a", newline="")
    with file:
        header = [
            "GUID",
            "Date",
            "Beginning_Balance",
            "Repayment_Amount",
            "Interest",
            "Principal",
            "Ending_Balance",
        ]
        writer = csv.DictWriter(file, fieldnames=header)

        writer.writerow(
            {
                "GUID": guid,
                "Date": repayment_date,
                "Beginning_Balance": beginning_balance,
                "Repayment_Amount": repayment_amount,
                "Interest": interest,
                "Principal": repayment_amount - interest,
                "Ending_Balance": ending_balance,
            }
        )
    file.close()
    logging.info("✅ Wrote some lines successfully")


def calculate_all_paydown_schedules(filepath: str) -> None:
    # Call the Holiday Hash Map creation for this script, load in the data that is to be worked with
    df_dim_loan, df_holidays = read_data_from_snowflake()

    holiday_schedule = all_known_holidays(df_holidays)

    # Write out a header column to make the csv easier to read
    file = open("paydown_schedule_test.csv", "a", newline="")
    with file:
        header = [
            "GUID",
            "Date",
            "Beginning_Balance",
            "Repayment_Amount",
            "Interest",
            "Principal",
            "Ending_Balance",
        ]
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()

    # The big loop - this will do the calculations for each of the amortization values needed in the final csv
    # The time complexity of this is expected to be O(n^2) given the loop inside a loop
    for index, row in df_dim_loan.iterrows():

        # Only looks at pending loans/not fully paid off loans and collects the information for each GUID
        if row["PRINCIPAL_AMOUNT"] > 0:
            number_of_pay_cycles = int(
                row["PRINCIPAL_AMOUNT"] / row["REPAYMENT_AMOUNT"]
            )
            principal = row["PRINCIPAL_AMOUNT"]
            repayment_amount = row["REPAYMENT_AMOUNT"]
            interval = schedule_in_days(
                row["REPAYMENT_SCHEDULE"]
            )  # This is the timeseries interval
            repayment_date = row["ACTIVATED_AT"].date()
            guid = row["GUID"]
            interest_percent_per_cycle = row["APR"] / (
                365 / interval_float(row["REPAYMENT_SCHEDULE"])
            )  # This is the float interval
            logging.info(" ✅ Collected all the values to proceed with calculations")
            for i in range(0, number_of_pay_cycles):
                beginning_balance = principal
                principal = principal - repayment_amount
                ending_balance = principal
                repayment_date = repayment_date + interval
                interest = beginning_balance * interest_percent_per_cycle
                logging.info(" ✅ Completed the calculations required")
                # Check the hash map to see if the date is a holiday or not
                while repayment_date in holiday_schedule.keys():
                    repayment_date = repayment_date + timedelta(days=1)
                    logging.info("✅ Checked the Hash Map for the date")
                # Finally; write the data to a csv to save the result of this calculation.
                write_data_to_csv(
                    guid,
                    repayment_date,
                    beginning_balance,
                    repayment_amount,
                    interest,
                    ending_balance,
                    "../data/paydown_schedule.csv",
                )
                logging.info("✅ Wrote the required data to the target location")
        else:
            continue


# calculate_all_paydown_schedules("../data/dim_loan.csv")
# Airflow constructors - this will make sure that it is a fully automated process and is run on the cloud T.T

# def create_dag() -> DAG:
#     with DAG(
#         dag_id="paydown_schedule",
#         start_date=pendulumn.datetime(
#             2020, 4, 1, tzinfo=pendulum.timezone("America/Toronto")
#         ),
#         schedule_interval="0 0 1 1 1",
#         default_args={
#             "retries": 3,
#             "retry_delay": timedelta(minutes=5),
#             "on_failure_callback": slack_task("slack_data_alerts"),
#         },
#         catchup=False,
#         max_active_runs=1,
#         on_failure_callback=slack_dag("slack_data_alerts"),
#     ) as dag:
#         calculate_all_paydown_schedules() = PythonOperator(
#             task_id="paydown_schedule",
#             python_callable=calculate_all_paydown_schedules('../data/dim_loan.csv'),
#         )
#         dag << estimated_lms_repayment_schedules
#     return dag
