import os
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

from typing import Dict, Any, cast

from helpers.auto_arima_parameters import AutoArimaParameters, ArimaProjectionParameters

from snowflake.sqlalchemy import VARIANT
from sqlalchemy.sql import select, func, text, literal_column
from sqlalchemy import Table, MetaData, Column, VARCHAR, Date, DateTime


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
        Column("parameters_hash", VARCHAR, nullable=False),
        Column("generated_at", DateTime, nullable=False),
        schema=schema,
    )

    cash_flow_projections.create(snowflake_engine, checkfirst=True)


def calculate_balance_projection(df: pd.DataFrame, prediction_df: pd.DataFrame) -> None:
    # We want to prevent negative balance projections
    running_balance = max(
        0, df["balance"].iloc[-1] + df["credits"].iloc[-1] - df["debits"].iloc[-1]
    )

    projected_opening_balances = []
    projected_opening_balances.append(running_balance)

    for index in range(1, len(prediction_df.index)):
        running_balance = max(
            0,
            running_balance
            + prediction_df["credits_prediction"].iloc[index]
            - prediction_df["debits_prediction"].iloc[index],
        )

        projected_opening_balances.append(running_balance)

    prediction_df["balance_prediction"] = projected_opening_balances


def calculate_projection(
    cash_flow_df: pd.DataFrame,
    auto_arima_params: AutoArimaParameters,
    arima_projection_params: ArimaProjectionParameters,
) -> pd.DataFrame:
    arima_debits_model = auto_arima(
        cash_flow_df["debits"], **(attr.asdict(auto_arima_params))
    )
    arima_credits_model = auto_arima(
        cash_flow_df["credits"], **(attr.asdict(auto_arima_params))
    )

    days_to_project = arima_projection_params.n_periods

    prediction_start_date = cash_flow_df.index.max() + timedelta(days=1)
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

    calculate_balance_projection(cash_flow_df, prediction_df)

    return prediction_df


def skip_projection(
    merchant_guid: str,
    account_guid: str,
    last_cash_flow_date: pd.Timestamp,
    parameters: Dict[str, Dict[str, Any]],
    existing_projections_df: pd.DataFrame,
) -> bool:
    try:
        existing_projections_df.loc[
            (
                merchant_guid,
                account_guid,
                last_cash_flow_date,
                sha256(json.dumps(parameters).encode("utf-8")).hexdigest(),
            )
        ]

        logging.info(
            f"⏩️️ Skipping generating projections for {merchant_guid} - {account_guid}"
        )
        return True
    except KeyError:
        return False


def apply_pre_projection_guardrails(
    cash_flow_df: pd.DataFrame, arima_projection_params: ArimaProjectionParameters
) -> bool:
    projection_weeks = int(
        arima_projection_params.n_periods / 7
    )  # Determine how many weeks we are going to project for
    required_cash_flow_weeks = projection_weeks * 3

    weekly_debits_credits_df = (
        cash_flow_df[["debits", "credits"]]
        .groupby(pd.Grouper(level="DateTime", freq="W-MON", label="left"))
        .sum()
    )

    non_zero_weeks = len(
        weekly_debits_credits_df[
            (weekly_debits_credits_df["credits"] > 0)
            & (weekly_debits_credits_df["debits"] > 0)
        ]
    )

    return required_cash_flow_weeks > non_zero_weeks


def apply_post_projection_guardrails(
    cash_flow_df: pd.DataFrame, projection_df: pd.DataFrame
) -> bool:
    closing_predicted_balance = (
        projection_df["balance_prediction"].iloc[-1]
        + projection_df["credits_prediction"].iloc[-1]
        - projection_df["debits_prediction"].iloc[-1]
    )

    # Validate closing balance
    min = cash_flow_df["balance"].min() - (2 * cash_flow_df["balance"].std())
    max = cash_flow_df["balance"].max() + (2 * cash_flow_df["balance"].std())

    logging.info(
        f"Min: {min}, Max: {max}, Projected balance: {closing_predicted_balance}"
    )

    return not (min <= closing_predicted_balance <= max)


def cash_flow_projection(
    merchant_guid: str,
    account_guid: str,
    cash_flow_df: pd.DataFrame,
    projection_id: str,
    existing_projections_df: pd.DataFrame,
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
) -> None:
    metadata = MetaData()
    zetatango_engine = SnowflakeHook(
        snowflake_zetatango_connection
    ).get_sqlalchemy_engine()
    auto_arima_parameters = AutoArimaParameters()
    arima_projection_parameters = ArimaProjectionParameters()

    details = {
        "id": projection_id,
        "version": os.environ.get("CI_PIPELINE_ID"),
        "auto_arima_params": attr.asdict(auto_arima_parameters),
        "arima_projection_params": attr.asdict(arima_projection_parameters),
    }

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
        cash_flow_df.index.max(),
        parameters_to_hash,
        existing_projections_df,
    ):
        return

    if apply_pre_projection_guardrails(cash_flow_df, arima_projection_parameters):
        logging.warning(
            f"❌ Pre projection guardrail for {merchant_guid} - {account_guid} applied"
        )

        return

    projection_df = calculate_projection(
        cash_flow_df, auto_arima_parameters, arima_projection_parameters,
    )

    if apply_post_projection_guardrails(cash_flow_df, projection_df):
        logging.warning(
            f"❌ Post projection guardrail for {merchant_guid} - {account_guid} applied"
        )

        return

    details["data"] = projection_df.to_dict()

    cash_flow_projections = Table(
        "cash_flow_projections",
        metadata,
        autoload=True,
        autoload_with=zetatango_engine,
        schema=zetatango_schema,
    )

    with zetatango_engine.begin() as tx:
        select_query = select(
            columns=[
                literal_column(f"'{merchant_guid}'").label("merchant_guid"),
                literal_column(f"'{account_guid}'").label("account_guid"),
                literal_column(f"'{cash_flow_df.index.max().date()}'").label(
                    "last_cash_flow_date"
                ),
                literal_column(
                    f"'{sha256(json.dumps(parameters_to_hash).encode('utf-8')).hexdigest()}'"
                ).label("parameters_hash"),
                func.parse_json(json.dumps(details)).label("projections"),
                func.CURRENT_TIMESTAMP().label("generated_at"),
            ]
        )

        insert_query = cash_flow_projections.insert().from_select(
            [
                "merchant_guid",
                "account_guid",
                "last_cash_flow_date",
                "parameters_hash",
                "projections",
                "generated_at",
            ],
            select_query,
        )

        tx.execute(insert_query)

    logging.info(
        f"✔️ Successfully stored projections for {merchant_guid} - {account_guid}"
    )


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

    select_query = select(
        columns=[
            cash_flow_projections.c.merchant_guid,
            cash_flow_projections.c.account_guid,
            cash_flow_projections.c.last_cash_flow_date,
            cash_flow_projections.c.parameters_hash,
            text("1"),
        ],
        from_obj=cash_flow_projections,
    )

    existing_projections_df = pd.read_sql_query(
        select_query,
        zetatango_engine,
        index_col=[
            "merchant_guid",
            "account_guid",
            "last_cash_flow_date",
            "parameters_hash",
        ],
    )

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
            .limit(1)
        )

        projection_id: str = sha256(
            f"{task_instance_key_str}_{ts_nodash}".encode("utf-8")
        ).hexdigest()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for row in tx.execute(statement).fetchall():
                df = pd.DataFrame.from_dict(
                    json.loads(row["daily_cash_flow"]), orient="index"
                )

                df.index = pd.to_datetime(df.index)

                full_index = pd.date_range(df.index.min(), df.index.max())
                df = df.reindex(full_index, fill_value=0)

                df.index.rename(inplace=True, name="DateTime")
                df.sort_index(inplace=True)

                executor.submit(
                    cash_flow_projection,
                    row["merchant_guid"],
                    row["account_guid"],
                    df,
                    projection_id,
                    existing_projections_df,
                    snowflake_zetatango_connection,
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
