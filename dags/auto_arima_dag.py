import os
import tempfile
import sys
import logging
import json
import pendulum
import pandas as pd
import secrets

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from concurrent.futures.thread import ThreadPoolExecutor
from pmdarima.arima import auto_arima

from utils import random_identifier
from pathlib import Path

from typing import Dict, Any, Tuple


def create_table(snowflake_connection: str) -> None:
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        create_table_query = (
            'CREATE TABLE IF NOT EXISTS "ZETATANGO"."CORE_PRODUCTION"."CASH_FLOW_PROJECTIONS"'
            " (MERCHANT_GUID VARCHAR NOT NULL, ACCOUNT_GUID VARCHAR NOT NULL,"
            " PROJECTIONS VARIANT NOT NULL, LAST_CASH_FLOW_DATE DATE NOT NULL,"
            " PARAMETERS_HASH NUMBER(19,0) NOT NULL, GENERATED_AT DATETIME NOT NULL)"
        )

        with snowflake_engine.begin() as tx:
            tx.execute(create_table_query).fetchall()
    except:
        e = sys.exc_info()
        logging.error(f"❌ Error creating table: {e}")
    finally:
        snowflake_engine.dispose()


def algorithm_details() -> Dict[str, Any]:
    # TODO: Make all these parameters config params so we can update without a code change
    auto_arima_params = {
        "start_p": 0,
        "max_p": 5,
        "start_q": 0,
        "max_q": 5,
        "max_d": 2,
        "start_P": 0,
        "max_P": 2,
        "start_Q": 0,
        "max_Q": 2,
        "max_D": 1,
        "max_order": 5,
        "stepwise": True,
        "alpha": 0.2,
        "supress_warnings": True,
        "random": True,
        "n_fits": 50,
        "random_state": 20,
    }

    arima_projection_params = {"n_periods": 14, "alpha": 0.2}

    return {
        "id": secrets.token_hex(32),
        "version": os.environ.get("CI_PIPELINE_ID"),
        "auto_arima_params": auto_arima_params,
        "arima_projection_params": arima_projection_params,
    }


def calculate_projection(
    data: Dict, auto_arima_params: Dict, arima_projection_params: Dict
) -> Tuple[str, Dict[str, Any]]:
    df = pd.DataFrame.from_dict(data, orient="index")
    df = df[["credits", "debits"]]

    df.index = pd.to_datetime(df.index)

    full_index = pd.date_range(df.index.min(), df.index.max())
    df = df.reindex(full_index, fill_value=0)

    arima_debits_model = auto_arima(df["debits"], **auto_arima_params)
    arima_credits_model = auto_arima(df["credits"], **auto_arima_params)

    days_to_project = arima_projection_params["n_periods"]

    prediction_start_date = df.index.max() + timedelta(days=1)
    prediction_index = pd.date_range(
        prediction_start_date,
        prediction_start_date + timedelta(days=(days_to_project - 1)),
    )

    predictions = {
        "credits prediction": arima_credits_model.predict(**arima_projection_params),
        "debits prediction": arima_debits_model.predict(**arima_projection_params),
    }

    prediction_df = pd.DataFrame(predictions, index=prediction_index)
    prediction_df.index = pd.to_datetime(prediction_df.index).astype(str)

    return df.index.max(), prediction_df.to_dict()


def cash_flow_projection(
    merchant_guid: str,
    account_guid: str,
    daily_cash_flow: str,
    snowflake_connection: str,
) -> None:
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()

        details = algorithm_details()
        cash_flow_data = json.loads(daily_cash_flow)

        results = calculate_projection(
            cash_flow_data,
            details["auto_arima_params"],
            details["arima_projection_params"],
        )

        last_cash_flow_date = results[0]
        details["data"] = results[1]

        parameters_to_hash = {
            "auto_arima_params": details["auto_arima_params"],
            "arima_projection_params": details["arima_projection_params"],
        }

        with snowflake_engine.begin() as tx, tempfile.TemporaryDirectory() as path:
            file_identifier = random_identifier()

            tmp_file_path = Path(path) / file_identifier
            staging_location = f'"ZETATANGO"."CORE_PRODUCTION"."{file_identifier}"'

            with open(tmp_file_path, "w") as file:
                file.write(json.dumps(details))

            statements = [
                f"CREATE OR REPLACE TEMPORARY STAGE {staging_location} FILE_FORMAT=(TYPE=JSON)",
                f"PUT file://{tmp_file_path} @{staging_location}",
                (
                    f"MERGE INTO"
                    f'  "ZETATANGO"."CORE_PRODUCTION"."CASH_FLOW_PROJECTIONS" cash_flow_projections'
                    f" USING (SELECT '{merchant_guid}' as merchant_guid,"
                    f"       '{account_guid}' as account_guid,"
                    f"       '{last_cash_flow_date}' as last_cash_flow_date,"
                    f"       HASH('{json.dumps(parameters_to_hash)}') as parameters_hash,"
                    f"       PARSE_JSON($1) AS projection FROM @{staging_location}) projection"
                    f" ON cash_flow_projections.merchant_guid = projection.merchant_guid and"
                    f"    cash_flow_projections.account_guid = projection.account_guid and"
                    f"    cash_flow_projections.last_cash_flow_date = projection.last_cash_flow_date and"
                    f"    cash_flow_projections.parameters_hash = projection.parameters_hash"
                    f" WHEN NOT MATCHED THEN INSERT"
                    f"   (merchant_guid, account_guid, last_cash_flow_date, parameters_hash,"
                    f"   generated_at, projections)"
                    f" VALUES (projection.merchant_guid, projection.account_guid,"
                    f"        projection.last_cash_flow_date, projection.parameters_hash,"
                    f"        CURRENT_TIMESTAMP, projection.projection)"
                ),
            ]

            for statement in statements:
                tx.execute(statement).fetchall()

            os.remove(tmp_file_path)

        logging.info(
            f"✔️ Successfully stored projections for {merchant_guid} - {account_guid}"
        )
    except:
        e = sys.exc_info()
        logging.error(
            f"❌ Error calculating projections for {merchant_guid} - {account_guid}: {e}"
        )
    finally:
        snowflake_engine.dispose()


def generate_projections(snowflake_connection: str, num_threads: int = 4) -> None:
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        cash_flow_query = """
            WITH object_debits_credits AS (
              SELECT account_guid, merchant_guid, date,
                     OBJECT_CONSTRUCT('debits', debits, 'credits', credits, 'balance',
                                     opening_balance) AS object
              FROM "ANALYTICS_PRODUCTION"."DBT_ARIO"."FCT_DAILY_BANK_ACCOUNT_BALANCE"
              ORDER BY date DESC
            )
            SELECT merchant_guid, account_guid, OBJECT_AGG(date::varchar, object) AS daily_cash_flow
            FROM object_debits_credits
            GROUP BY merchant_guid, account_guid"""

        # Get the set of downloaded flinks responses
        cash_flow = pd.read_sql_query(cash_flow_query, snowflake_engine)

        for _index, row in cash_flow.iterrows():
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                executor.submit(
                    cash_flow_projection,
                    row["merchant_guid"],
                    row["account_guid"],
                    row["daily_cash_flow"],
                    snowflake_connection,
                )

    except:
        e = sys.exc_info()
        logging.error(f"❌ Error accessing daily cash flow: {e}")
    finally:
        snowflake_engine.dispose()


def create_dag() -> DAG:
    with DAG(
        dag_id="generate_cash_flow_projections",
        start_date=pendulum.datetime(
            2020, 8, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="30 0,10-22/4 * * *",
        default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    ) as dag:
        dag << PythonOperator(
            task_id="create_table",
            python_callable=create_table,
            op_kwargs={"snowflake_connection": "snowflake_zetatango_production",},
        ) >> PythonOperator(
            task_id="generate_projections",
            python_callable=generate_projections,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "num_threads": 10,
            },
        )

        return dag


if __name__ == "__main__":
    from unittest.mock import patch
    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    role = os.environ.get("SNOWFLAKE_ROLE")
    user = os.environ.get("SNOWFLAKE_USER")

    url = (
        URL(account=account, database=database, role=role, user=user)
        if user
        else URL(account=account, database=database, role=role)
    )

    with patch(
        "dags.auto_arima_dag.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            url, connect_args={"authenticator": "externalbrowser",},
        ),
    ) as mock_engine:
        create_table("snowflake_connection")
        generate_projections("snowflake_connection", 1)
else:
    globals()["generate_cash_flow_projections"] = create_dag()
