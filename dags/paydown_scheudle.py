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

# to-do Need to collect the appropriate data to work with from ❄️ look @ dim_loan and holidays and repayment_amount
# def read_data_from_snowflake():
# Pretty sure that we need to connect to snowflake to pull whatever data is going to be used here for full automation
# Expected Steps for this function:
# 1.) Connect to snowflake
# 2.) Pull the table(s) of interest
# 3.) Load the tables into memory (if possible)
# 4.) Return the tables for use in the paydown calculation
# 5.) NB: This might need to be done in a chunkwise fashion as the data input may be too large
# logging.info(f"Successfully connected to ❄️ for data collection")
# pass


# This function just pulls in data from disk to a pandas df object and then processes with pandas for the table
# Can be adjusted for PyArrow compatability at a later point in time if needed.
# NB: This is not a permanent function - will be replaced by read_data_from_snowflake for production
def read_temp_csv_data(filepath: str) -> pd.dataframe:
    if "dim_loan" in filepath:
        df = pd.read_csv(filepath)
        # Pre-Processing for later in the pipe
        fields_for_use = [
            "GUID",
            "ACTIVATED_AT",
            "APR",
            "PRINCIPAL_AMOUNT",
            "REPAYMENT_AMOUNT",
            "TOTAL_REPAYMENTS_AMOUNT",
            "INTEREST_AMOUNT",
            "REPAYMENT_SCHEDULE",
            "STATE",
            "REMAINING_PRINCIPAL",
            "INTEREST_BALANCE",
            "OUTSTANDING_BALANCE",
        ]
        df = df[fields_for_use]
        df["ACTIVATED_AT"] = pd.to_datetime(df["ACTIVATED_AT"])
        logging.info(f"✅ Processed the {filepath} data")
    elif "holiday" in filepath:
        df = pd.read_csv(filepath)
        df["date"] = pd.to_datetime(df["date"])
        for i, row in df.iterrows():
            df.at[i, "date"] = row["date"].date()
        logging.info(f"✅ Processed the {filepath} data")
    else:
        df = False
        logging.info(
            f"❌ Could not process the {filepath} data, many errors to follow..."
        )
    return df


def all_known_holidays(holiday_filepath: str) -> dict:
    df = read_temp_csv_data(holiday_filepath)
    # Successfully makes a dictionary (hash map) of all holiday data provided in the read in csv (SQL statement)
    # Ensure that date lookup becomes O(1) instead of O(n^2)
    holiday_hash = dict(zip(df.date, df.is_holiday))
    logging.info("✅ Collected all the Holiday info and made a hash map ")
    return holiday_hash


# Schedule in days will return a datetime object to show how many days are in between different payment schedules
def schedule_in_days(frequency: str) -> timedelta:
    try:
        if frequency == "daily":
            return timedelta(days=1)
        elif frequency == "weekly":
            return timedelta(days=7)
        elif frequency == "bi-weekly":
            return timedelta(days=14)
    except ValueError:
        logging.info(
            f" ❌ Likely an issue in the data present; data did not fit daily/weekly/bi-weekly as expected"
        )


def interval_float(frequency: str) -> float:
    try:
        if frequency == "daily":
            return float(1)
        elif frequency == "weekly":
            return float(7)
        elif frequency == "bi-weekly":
            return float(14)
    except ValueError:
        logging.info(
            f" ❌ Likely an issue in the data present; data did not fit daily/weekly/bi-weekly as expected"
        )


def write_data_to_csv(
    guid: str,
    repayment_date: datetime.date,
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
    logging.info(f"✅ Wrote some lines successfully")


def calculate_all_paydown_schedules(filepath: str) -> None:
    # Call the Holiday Hash Map creation for this script, load in the data that is to be worked with
    holiday_schedule = all_known_holidays(
        "../data/holiday.csv"
    )  # This can probably be changed to a variable name later
    df = read_temp_csv_data(filepath)

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
    for index, row in df.iterrows():

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
            logging.info(f" ✅ Collected all the values to proceed with calculations")
            for i in range(0, number_of_pay_cycles):
                beginning_balance = principal
                principal = principal - repayment_amount
                ending_balance = principal
                repayment_date = repayment_date + interval
                interest = beginning_balance * interest_percent_per_cycle
                logging.info(f" ✅ Completed the calculations required")
                # Check the hash map to see if the date is a holiday or not
                while repayment_date in holiday_schedule.keys():
                    repayment_date = repayment_date + timedelta(days=1)
                    logging.info(f"✅ Checked the Hash Map for {repayment_date}")
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
                logging.info(f"✅ Wrote the required data to the target location")
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
