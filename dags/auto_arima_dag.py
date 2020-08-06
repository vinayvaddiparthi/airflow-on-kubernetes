import os
import tempfile
import sys
import logging
import json
import pendulum
import pandas as pd
import attr

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from concurrent.futures.thread import ThreadPoolExecutor
from pmdarima.arima import auto_arima
from hashlib import sha256

from utils import random_identifier
from pathlib import Path

from typing import Dict, Any, cast

from helpers.auto_arima_parameters import AutoArimaParameters, ArimaProjectionParameters

from snowflake.sqlalchemy import VARIANT
from sqlalchemy.sql import select, func, text
from sqlalchemy import Table, MetaData, Column, VARCHAR, Date, Numeric, DateTime


def create_table(snowflake_connection: str, schema: str,) -> None:
    metadata = MetaData()
    snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()

    cash_flow_projections = Table(
        "cash_flow_projections",
        metadata,
        Column("merchant_guid", VARCHAR, nullable=False),
        Column("account_guid", VARCHAR, nullable=False),
        Column("projections", VARIANT, nullable=False),
        Column("last_cash_flow_date", Date, nullable=False),
        Column("parameters_hash", Numeric(19, 0), nullable=False),
        Column("generated_at", DateTime, nullable=False),
        schema=schema,
    )

    cash_flow_projections.create(snowflake_engine, checkfirst=True)


def calculate_balance_projection(df: pd.DataFrame, prediction_df: pd.DataFrame) -> None:
    running_balance = (
        df["balance"].iloc[-1] + df["credits"].iloc[-1] - df["debits"].iloc[-1]
    )

    projected_opening_balances = []
    projected_opening_balances.append(running_balance)

    for index in range(1, len(prediction_df.index)):
        running_balance = (
            running_balance
            + prediction_df["credits_prediction"].iloc[index]
            - prediction_df["debits_prediction"].iloc[index]
        )

        projected_opening_balances.append(running_balance)

    prediction_df["balance_prediction"] = projected_opening_balances


def find_last_cash_flow_date(data: Dict) -> pd.Timestamp:
    df = pd.DataFrame.from_dict(data, orient="index")

    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    return df.index.max()


def calculate_projection(
    data: Dict,
    auto_arima_params: AutoArimaParameters,
    arima_projection_params: ArimaProjectionParameters,
) -> Dict[str, Any]:
    df = pd.DataFrame.from_dict(data, orient="index")

    df.index = pd.to_datetime(df.index)

    full_index = pd.date_range(df.index.min(), df.index.max())
    df = df.reindex(full_index, fill_value=0)

    df.sort_index(inplace=True)

    arima_debits_model = auto_arima(df["debits"], **(attr.asdict(auto_arima_params)))
    arima_credits_model = auto_arima(df["credits"], **(attr.asdict(auto_arima_params)))

    days_to_project = arima_projection_params.n_periods

    prediction_start_date = df.index.max() + timedelta(days=1)
    prediction_index = pd.date_range(
        prediction_start_date,
        prediction_start_date + timedelta(days=(days_to_project - 1)),
    )

    predictions = {
        "credits_prediction": arima_credits_model.predict(
            **(attr.asdict(arima_projection_params))
        ),
        "debits_prediction": arima_debits_model.predict(
            **(attr.asdict(arima_projection_params))
        ),
    }

    prediction_df = pd.DataFrame(predictions, index=prediction_index)
    prediction_df.index = pd.to_datetime(prediction_df.index).astype(str)

    calculate_balance_projection(df, prediction_df)

    return prediction_df.to_dict()


def skip_projection(
    merchant_guid: str,
    account_guid: str,
    last_cash_flow_date: pd.Timestamp,
    parameters: Dict[str, Dict[str, Any]],
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
) -> bool:
    metadata = MetaData()
    zetatango_engine = SnowflakeHook(
        snowflake_zetatango_connection
    ).get_sqlalchemy_engine()

    cash_flow_projections = Table(
        "cash_flow_projections",
        metadata,
        autoload=True,
        autoload_with=zetatango_engine,
        schema=zetatango_schema,
    )

    select_query = (
        select(columns=[text("1")], from_obj=cash_flow_projections)
        .where(cash_flow_projections.c.merchant_guid == merchant_guid)
        .where(cash_flow_projections.c.account_guid == account_guid)
        .where(
            cash_flow_projections.c.last_cash_flow_date == last_cash_flow_date.date()
        )
        .where(
            cash_flow_projections.c.parameters_hash == func.hash(json.dumps(parameters))
        )
    )

    with zetatango_engine.begin() as tx:
        results = tx.execute(select_query).fetchall()

        if len(results) > 0:
            logging.info(
                f"⏩️️ Skipping generating projections for {merchant_guid} - {account_guid}"
            )
            return True
        else:
            return False


def cash_flow_projection(
    merchant_guid: str,
    account_guid: str,
    daily_cash_flow: str,
    snowflake_connection: str,
    projection_id: str,
    schema: str,
) -> None:
    try:
        snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        auto_arima_parameters = AutoArimaParameters()
        arima_projection_parameters = ArimaProjectionParameters()

        details = {
            "id": projection_id,
            "version": os.environ.get("CI_PIPELINE_ID"),
            "auto_arima_params": attr.asdict(auto_arima_parameters),
            "arima_projection_params": attr.asdict(arima_projection_parameters),
        }
        cash_flow_data = json.loads(daily_cash_flow)

        last_cash_flow_date = find_last_cash_flow_date(cash_flow_data)

        parameters_to_hash: Dict[str, Dict[str, Any]] = {
            "auto_arima_params": cast(Dict[str, Any], details["auto_arima_params"]),
            "arima_projection_params": cast(
                Dict[str, Any], details["arima_projection_params"]
            ),
        }
        parameters_to_hash["auto_arima_params"].pop("random_state", None)

        if skip_projection(
            merchant_guid,
            account_guid,
            last_cash_flow_date,
            parameters_to_hash,
            snowflake_connection,
            schema,
        ):
            return

        details["data"] = calculate_projection(
            cash_flow_data, auto_arima_parameters, arima_projection_parameters,
        )

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


def generate_projections(
    snowflake_zetatango_connection: str,
    snowflake_analytics_connection: str,
    num_threads: int,
    task_instance_key_str: str,
    ts_nodash: str,
    analytics_schema: str,
    zetatango_schema: str,
    **kwargs: Any,
) -> None:
    metadata = MetaData()
    production_engine = SnowflakeHook(
        snowflake_analytics_connection
    ).get_sqlalchemy_engine()

    fct_daily_bank_account_balance = Table(
        "fct_daily_bank_account_balance",
        metadata,
        autoload=True,
        autoload_with=production_engine,
        schema=analytics_schema,
    )

    with production_engine.begin() as tx:
        table_query = (
            select(
                columns=[
                    fct_daily_bank_account_balance.c.merchant_guid,
                    fct_daily_bank_account_balance.c.account_guid,
                    fct_daily_bank_account_balance.c.date,
                    func.object_construct(
                        text("'debits'"),
                        fct_daily_bank_account_balance.c.debits,
                        text("'credits'"),
                        fct_daily_bank_account_balance.c.credits,
                        text("'balance'"),
                        fct_daily_bank_account_balance.c.opening_balance,
                    ).label("object"),
                ],
                from_obj=fct_daily_bank_account_balance,
            )
            .order_by(fct_daily_bank_account_balance.c.date.desc())
            .cte("object_debits_credits")
        )

        statement = (
            select(
                columns=[
                    table_query.c.merchant_guid,
                    table_query.c.account_guid,
                    func.object_agg(table_query.c.date, table_query.c.object).label(
                        "daily_cash_flow"
                    ),
                ]
            )
            .group_by(table_query.c.merchant_guid, table_query.c.account_guid)
            .select_from(table_query)
        )

        projection_id: str = sha256(
            f"{task_instance_key_str}_{ts_nodash}".encode("utf-8")
        ).hexdigest()

        for row in tx.execute(statement).fetchall():
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                executor.submit(
                    cash_flow_projection,
                    row["merchant_guid"],
                    row["account_guid"],
                    row["daily_cash_flow"],
                    snowflake_zetatango_connection,
                    projection_id,
                    zetatango_schema,
                )


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
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "schema": "CORE_PRODUCTION",
            },
        ) >> PythonOperator(
            task_id="generate_projections",
            python_callable=generate_projections,
            provide_context=True,
            op_kwargs={
                "snowflake_zetatango_connection": "snowflake_zetatango_production",
                "snowflake_analytics_connection": "snowflake_analytics_production",
                "num_threads": 10,
                "analytics_schema": "DBT_ARIO",
                "zetatango_schema": "CORE_PRODUCTION",
            },
        )

        return dag


if __name__ == "__main__":
    from tests.helpers.snowflake_hook import test_get_sqlalchemy_engine

    # Monkeypatch the get engine function to return the right engine depending on the connection string
    SnowflakeHook.get_sqlalchemy_engine = test_get_sqlalchemy_engine

    create_table("snowflake_zetatango_production", "CORE_STAGING")
#     generate_projections(
#         "snowflake_zetatango_production",
#         "snowflake_analytics_connection",
#         1,
#         "task_id",
#         "task_ts",
#         "DBT_ARIO",
#         "CORE_STAGING",
#     )
else:
    globals()["generate_cash_flow_projections"] = create_dag()
