import logging
import json
import pendulum
import pandas as pd
import attr
import sys
import sqlalchemy
import datetime

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from datetime import timedelta
from concurrent.futures.thread import ThreadPoolExecutor
from pmdarima.arima import auto_arima
from hashlib import sha256
from math import sqrt

from typing import Dict, Any, cast, List

from helpers.cash_flow_helper import (
    calculate_opening_balances,
    guardrail_linear_regression,
)
from helpers.auto_arima_parameters import (
    AutoArimaParameters,
    ArimaProjectionParameters,
    CashFlowProjectionParameters,
)

from sqlalchemy.sql import select, func, text, literal_column, literal, outerjoin
from sqlalchemy.engine import RowProxy
from sqlalchemy.sql.selectable import Select
from snowflake.sqlalchemy import VARIANT
from sqlalchemy import Table, MetaData, Date, case, Numeric

from helpers.rabbit_mq_helper import notify_subscribers
from utils.failure_callbacks import slack_dag


def calculate_balance_projection(
    df: pd.DataFrame, prediction_df: pd.DataFrame, balance_spreads: List[float]
) -> None:
    opening_balance = (
        df["balance"].iloc[-1] + df["credits"].iloc[-1] - df["debits"].iloc[-1]
    )

    prediction_df["balance_prediction"] = calculate_opening_balances(
        opening_balance, prediction_df, "credits_prediction", "debits_prediction"
    )

    lo_balances = []
    hi_balances = []

    for opening_balance, balance_spread in zip(
        prediction_df["balance_prediction"], balance_spreads
    ):
        lo_balance = opening_balance - balance_spread
        hi_balance = opening_balance + balance_spread

        lo_balances.append(lo_balance)
        hi_balances.append(hi_balance)

    prediction_df["lo_balance_prediction"] = lo_balances
    prediction_df["hi_balance_prediction"] = hi_balances


def calculate_balance_spreads(
    credits_confidence_intervals: List[List[float]],
    debits_confidence_intervals: List[List[float]],
) -> List[float]:
    f = 1.281552
    balance_spreads = []

    for credits_interval, debits_interval in zip(
        credits_confidence_intervals, debits_confidence_intervals
    ):
        lo_c80, hi_c80 = credits_interval
        lo_d80, hi_d80 = debits_interval

        spread_credits = (hi_c80 - lo_c80) / 2 / f
        spread_debits = (hi_d80 - lo_d80) / 2 / f

        spread_balance = sqrt(spread_credits ** 2 + spread_debits ** 2) * f

        balance_spreads.append(spread_balance)

    return balance_spreads


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

    credits_prediction, credits_confidence_intervals = arima_credits_model.predict(
        **(attr.asdict(arima_projection_params))
    )
    debits_prediction, debits_confidence_intervals = arima_debits_model.predict(
        **(attr.asdict(arima_projection_params))
    )

    predictions = {
        "credits_prediction": credits_prediction,
        "debits_prediction": debits_prediction,
    }

    prediction_df = pd.DataFrame(predictions, index=prediction_index)

    # We zero out any prediction that is negative before calculating balance
    prediction_df.clip(lower=0, inplace=True)

    calculate_balance_projection(
        cash_flow_df,
        prediction_df,
        calculate_balance_spreads(
            credits_confidence_intervals, debits_confidence_intervals
        ),
    )

    return prediction_df


def skip_multi_account_projection(
    merchant_guid: str,
    account_guids: List[str],
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
    parameters: Dict[str, Dict[str, Any]],
) -> bool:
    metadata = MetaData()
    zetatango_engine = SnowflakeHook(
        snowflake_zetatango_connection
    ).get_sqlalchemy_engine()

    multi_account_cash_flow_projections = Table(
        "multi_account_cash_flow_projections",
        metadata,
        autoload=True,
        autoload_with=zetatango_engine,
        schema=zetatango_schema,
    )

    select_query = select(
        columns=[
            multi_account_cash_flow_projections.c.merchant_guid,
            multi_account_cash_flow_projections.c.parameters_hash,
            text("1"),
        ],
        from_obj=multi_account_cash_flow_projections,
    ).where(
        multi_account_cash_flow_projections.c.merchant_guid == literal(merchant_guid)
    )

    existing_projections_df = pd.read_sql_query(
        select_query,
        zetatango_engine,
        index_col=[
            "merchant_guid",
            "parameters_hash",
        ],
    )

    try:
        existing_projections_df.loc[
            (
                merchant_guid,
                sha256(json.dumps(parameters).encode("utf-8")).hexdigest(),
            )
        ]

        logging.info(
            f"⏩️️ Skipping generating projections for {merchant_guid} - {account_guids}"
        )
        return True
    except KeyError:
        return False


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

    logging.info(
        f"Projection weeks: {projection_weeks}, Required weeks: {required_cash_flow_weeks}, Non-zero cash flow weeks: {non_zero_weeks}"
    )

    return required_cash_flow_weeks > non_zero_weeks


def apply_post_projection_guardrail_closing_balance(
    cash_flow_df: pd.DataFrame, projection_df: pd.DataFrame
) -> bool:
    projection_df.index = pd.to_datetime(projection_df.index)
    projection_df.index.rename(inplace=True, name="DateTime")
    projection_df.sort_index(inplace=True)

    weekly_actuals_df = (
        cash_flow_df[["debits", "credits"]]
        .groupby(pd.Grouper(level="DateTime", freq="W-MON", label="left"))
        .sum()
    )
    weekly_actuals_df["balance"] = calculate_opening_balances(
        cash_flow_df["balance"].iloc[0], weekly_actuals_df, "credits", "debits"
    )

    weekly_projections_df = (
        projection_df[["debits_prediction", "credits_prediction"]]
        .groupby(pd.Grouper(level="DateTime", freq="W-MON", label="left"))
        .sum()
    )

    weekly_projections_opening_balance = (
        weekly_actuals_df["balance"].iloc[-1]
        + weekly_actuals_df["credits"].iloc[-1]
        - weekly_actuals_df["debits"].iloc[-1]
    )
    weekly_projections_df["balance_prediction"] = calculate_opening_balances(
        weekly_projections_opening_balance,
        weekly_projections_df,
        "credits_prediction",
        "debits_prediction",
    )

    closing_predicted_balance = (
        weekly_projections_df["balance_prediction"].iloc[-1]
        + weekly_projections_df["credits_prediction"].iloc[-1]
        - weekly_projections_df["debits_prediction"].iloc[-1]
    )

    # Validate closing balance
    min_balance = cash_flow_df["balance"].min() - (2 * cash_flow_df["balance"].std())
    max_balance = cash_flow_df["balance"].max() + (2 * cash_flow_df["balance"].std())

    logging.info(
        f"Min weekly balance: {min_balance}, Max weekly balance: {max_balance}, Projected closing weekly balance: {closing_predicted_balance}"
    )

    return not (min_balance <= closing_predicted_balance <= max_balance)


def store_multi_account_projection(
    merchant_guid: str,
    account_guids: List[str],
    cash_flow_df: pd.DataFrame,
    parameters_to_hash: Dict[str, Dict[str, Any]],
    details: Dict[str, Any],
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
) -> None:
    metadata = MetaData()
    zetatango_engine = SnowflakeHook(
        snowflake_zetatango_connection
    ).get_sqlalchemy_engine()

    multi_account_cash_flow_projections = Table(
        "multi_account_cash_flow_projections",
        metadata,
        autoload=True,
        autoload_with=zetatango_engine,
        schema=zetatango_schema,
    )

    with zetatango_engine.begin() as tx:
        select_query = select(
            columns=[
                literal_column(f"'{merchant_guid}'").label("merchant_guid"),
                func.array_construct(*account_guids).label("account_guids"),
                literal_column(
                    f"'{sha256(json.dumps(parameters_to_hash).encode('utf-8')).hexdigest()}'"
                ).label("parameters_hash"),
                func.parse_json(json.dumps(details)).label("projections"),
                func.current_timestamp().label("generated_at"),
            ]
        )

        insert_query = multi_account_cash_flow_projections.insert().from_select(
            [
                "merchant_guid",
                "account_guids",
                "parameters_hash",
                "projections",
                "generated_at",
            ],
            select_query,
        )

        tx.execute(insert_query)

    logging.info(
        f"✔️ Successfully stored projections for {merchant_guid} - {account_guids}"
    )


def store_projection(
    merchant_guid: str,
    account_guid: str,
    cash_flow_df: pd.DataFrame,
    parameters_to_hash: Dict[str, Dict[str, Any]],
    details: Dict[str, Any],
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
) -> None:
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
                func.current_timestamp().label("generated_at"),
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


def do_projection(
    merchant_guid: str,
    account_guid: str,
    cash_flow_df: pd.DataFrame,
    projection_id: str,
    existing_projections_df: pd.DataFrame,
    snowflake_zetatango_connection: str,
    zetatango_schema: str,
) -> None:
    auto_arima_parameters = AutoArimaParameters()
    arima_projection_parameters = ArimaProjectionParameters()
    cash_flow_projection_parameters = CashFlowProjectionParameters()

    logging.info(
        f"Generating projections for {account_guid} for merchant {merchant_guid}"
    )

    details = {
        "id": projection_id,
        "version": cash_flow_projection_parameters.version,
        "auto_arima_params": attr.asdict(auto_arima_parameters),
        "arima_projection_params": attr.asdict(arima_projection_parameters),
    }

    parameters_to_hash: Dict[str, Dict[str, Any]] = {
        "auto_arima_params": cast(Dict[str, Any], details["auto_arima_params"]),
        "arima_projection_params": cast(
            Dict[str, Any], details["arima_projection_params"]
        ),
        "cash_flow_projection_parameters": cast(Dict[str, Any], details["version"]),
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

        store_projection(
            merchant_guid,
            account_guid,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    projection_df = calculate_projection(
        cash_flow_df,
        auto_arima_parameters,
        arima_projection_parameters,
    )

    if apply_post_projection_guardrail_closing_balance(cash_flow_df, projection_df):
        logging.warning(
            f"❌ Post projection guardrail on closing balance for {merchant_guid} - {account_guid} applied"
        )

        store_projection(
            merchant_guid,
            account_guid,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    if guardrail_linear_regression(cash_flow_df, projection_df):
        logging.warning(
            f"❌ Post projection guardrail on linear regression for {merchant_guid} - {account_guid} applied"
        )

        store_projection(
            merchant_guid,
            account_guid,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    projection_df.index = pd.to_datetime(projection_df.index).astype(str)
    details["data"] = projection_df.to_dict("index")

    store_projection(
        merchant_guid,
        account_guid,
        cash_flow_df,
        parameters_to_hash,
        details,
        snowflake_zetatango_connection,
        zetatango_schema,
    )


def first_common_transaction_date_query(
    snowflake_analytics_connection: str,
    analytics_schema: str,
    merchant_guid: str,
    account_guids: List[str],
) -> Select:
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

    first_transaction_by_account_query = (
        select(
            columns=[
                fct_daily_bank_account_balance.c.merchant_guid,
                fct_daily_bank_account_balance.c.account_guid,
                sqlalchemy.cast(
                    func.min(fct_daily_bank_account_balance.c.date), Date
                ).label("first_transaction"),
            ],
            from_obj=fct_daily_bank_account_balance,
        )
        .group_by(
            fct_daily_bank_account_balance.c.merchant_guid,
            fct_daily_bank_account_balance.c.account_guid,
        )
        .cte("first_transaction_by_account")
    )

    statement = (
        select(
            columns=[
                first_transaction_by_account_query.c.merchant_guid,
                func.max(first_transaction_by_account_query.c.first_transaction).label(
                    "first_common_transaction_date"
                ),
            ]
        )
        .where(first_transaction_by_account_query.c.merchant_guid == merchant_guid)
        .where(first_transaction_by_account_query.c.account_guid.in_(account_guids))
        .group_by(first_transaction_by_account_query.c.merchant_guid)
        .select_from(first_transaction_by_account_query)
    )

    return statement


def last_transaction_date_query(
    snowflake_analytics_connection: str,
    analytics_schema: str,
    merchant_guid: str,
    account_guids: List[str],
) -> Select:
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

    last_transaction_by_account_query = (
        select(
            columns=[
                fct_daily_bank_account_balance.c.merchant_guid,
                fct_daily_bank_account_balance.c.account_guid,
                func.max(fct_daily_bank_account_balance.c.date).label(
                    "last_transaction"
                ),
            ],
            from_obj=fct_daily_bank_account_balance,
        )
        .group_by(
            fct_daily_bank_account_balance.c.merchant_guid,
            fct_daily_bank_account_balance.c.account_guid,
        )
        .cte("last_transaction_by_account")
    )

    statement = (
        select(
            columns=[
                last_transaction_by_account_query.c.merchant_guid,
                func.object_agg(
                    last_transaction_by_account_query.c.account_guid,
                    sqlalchemy.cast(
                        last_transaction_by_account_query.c.last_transaction, VARIANT
                    ),
                ).label("last_transaction_dates"),
            ]
        )
        .where(last_transaction_by_account_query.c.merchant_guid == merchant_guid)
        .where(last_transaction_by_account_query.c.account_guid.in_(account_guids))
        .group_by(last_transaction_by_account_query.c.merchant_guid)
        .select_from(last_transaction_by_account_query)
    )

    return statement


def debits_credits_query(
    snowflake_analytics_connection: str,
    analytics_schema: str,
    merchant_guid: str,
    account_guids: List[str],
) -> Select:
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

    dim_date2 = Table(
        "dim_date2",
        metadata,
        autoload=True,
        autoload_with=production_engine,
        schema="DBT",
    )

    merchant_accounts = (
        select(
            columns=[
                fct_daily_bank_account_balance.c.merchant_guid,
                fct_daily_bank_account_balance.c.account_guid,
                func.min(fct_daily_bank_account_balance.c.date).label("min_date"),
                func.max(fct_daily_bank_account_balance.c.date).label("max_date"),
            ],
            from_obj=fct_daily_bank_account_balance,
        )
        .group_by(
            fct_daily_bank_account_balance.c.merchant_guid,
            fct_daily_bank_account_balance.c.account_guid,
        )
        .cte("merchant_accounts")
    )

    bank_account_days = (
        select(
            columns=[
                merchant_accounts.c.merchant_guid,
                merchant_accounts.c.account_guid,
                dim_date2.c.date_day,
            ],
            from_obj=[dim_date2, merchant_accounts],
        )
        .where(dim_date2.c.date_day >= merchant_accounts.c.min_date)
        .where(dim_date2.c.date_day <= merchant_accounts.c.max_date)
        .cte("bank_account_days")
    )

    fct_daily_bank_account_balance_join = outerjoin(
        bank_account_days,
        fct_daily_bank_account_balance,
        (
            bank_account_days.c.merchant_guid
            == fct_daily_bank_account_balance.c.merchant_guid
        )
        & (
            bank_account_days.c.account_guid
            == fct_daily_bank_account_balance.c.account_guid
        )
        & (bank_account_days.c.date_day == fct_daily_bank_account_balance.c.date),
    )

    bank_account_balance_by_day = (
        select(
            columns=[
                bank_account_days.c.merchant_guid,
                bank_account_days.c.account_guid,
                bank_account_days.c.date_day.label("date"),
                func.zeroifnull(fct_daily_bank_account_balance.c.credits).label(
                    "credits"
                ),
                func.zeroifnull(fct_daily_bank_account_balance.c.debits).label(
                    "debits"
                ),
                sqlalchemy.cast(
                    case(
                        [
                            (
                                fct_daily_bank_account_balance.c.opening_balance.isnot(
                                    None
                                ),
                                fct_daily_bank_account_balance.c.opening_balance,
                            )
                        ],
                        else_=text(
                            (
                                'lag("DBT_ARIO".fct_daily_bank_account_balance.opening_balance) IGNORE NULLS OVER('
                                " PARTITION BY bank_account_days.merchant_guid, bank_account_days.account_guid"
                                " ORDER BY bank_account_days.date_day DESC"
                                ")"
                            )
                        ),
                    ),
                    Numeric(37, 2),
                ).label("opening_balance"),
            ],
            from_obj=bank_account_days,
        )
        .select_from(fct_daily_bank_account_balance_join)
        .cte("bank_account_balance_by_day")
    )

    summed_accounts_query = (
        select(
            columns=[
                bank_account_balance_by_day.c.merchant_guid,
                func.array_construct(account_guids).label("account_guids"),
                bank_account_balance_by_day.c.date,
                func.sum(bank_account_balance_by_day.c.credits).label("sum_credits"),
                func.sum(bank_account_balance_by_day.c.debits).label("sum_debits"),
                func.sum(bank_account_balance_by_day.c.opening_balance).label(
                    "sum_opening_balance"
                ),
            ],
            from_obj=bank_account_balance_by_day,
        )
        .where(bank_account_balance_by_day.c.merchant_guid == merchant_guid)
        .where(bank_account_balance_by_day.c.account_guid.in_(account_guids))
        .group_by(
            bank_account_balance_by_day.c.merchant_guid,
            bank_account_balance_by_day.c.date,
        )
        .order_by(bank_account_balance_by_day.c.date.desc())
        .cte("summed_accounts")
    )

    object_debits_credits_query = (
        select(
            columns=[
                summed_accounts_query.c.merchant_guid,
                summed_accounts_query.c.account_guids,
                summed_accounts_query.c.date,
                func.object_construct(
                    text("'debits'"),
                    summed_accounts_query.c.sum_debits,
                    text("'credits'"),
                    summed_accounts_query.c.sum_credits,
                    text("'balance'"),
                    summed_accounts_query.c.sum_opening_balance,
                ).label("object"),
            ],
        )
        .select_from(summed_accounts_query)
        .order_by(summed_accounts_query.c.date.desc())
        .cte("object_debits_credits")
    )

    statement = (
        select(
            columns=[
                object_debits_credits_query.c.merchant_guid,
                object_debits_credits_query.c.account_guids,
                func.object_agg(
                    object_debits_credits_query.c.date,
                    object_debits_credits_query.c.object,
                ).label("daily_cash_flow"),
            ]
        )
        .group_by(
            object_debits_credits_query.c.merchant_guid,
            object_debits_credits_query.c.account_guids,
        )
        .select_from(object_debits_credits_query)
    )

    return statement


def generate_multi_projections(
    snowflake_zetatango_connection: str,
    snowflake_analytics_connection: str,
    task_instance_key_str: str,
    ts_nodash: str,
    analytics_schema: str,
    zetatango_schema: str,
    dag_run: DagRun,
    **kwargs: Any,
) -> None:
    production_engine = SnowflakeHook(
        snowflake_analytics_connection
    ).get_sqlalchemy_engine()

    if "merchant_guid" not in dag_run.conf or "account_ids" not in dag_run.conf:
        logging.error(
            "❌ Unable to execute multi projection without merchant_guid or account_ids"
        )

        return

    merchant_guid = dag_run.conf["merchant_guid"]
    account_guids = dag_run.conf["account_ids"]

    # Make sure account guids are sorted
    account_guids.sort()

    projection_id: str = sha256(
        f"{task_instance_key_str}_{ts_nodash}".encode("utf-8")
    ).hexdigest()

    with production_engine.begin() as tx:
        first_transaction_date = get_first_common_transaction_date(
            snowflake_analytics_connection,
            analytics_schema,
            merchant_guid,
            account_guids,
        )

        logging.info(
            f"First common transaction date for {merchant_guid} - {account_guids} is {first_transaction_date}"
        )

        debit_credits_statement = debits_credits_query(
            snowflake_analytics_connection,
            analytics_schema,
            merchant_guid,
            account_guids,
        )

        row = tx.execute(debit_credits_statement).first()
        cash_flow_df = format_cash_flow_into_df(row, first_transaction_date)

        try:
            do_multi_account_projection(
                merchant_guid,
                account_guids,
                cash_flow_df,
                projection_id,
                snowflake_zetatango_connection,
                snowflake_analytics_connection,
                analytics_schema,
                zetatango_schema,
            )
        except:
            e = sys.exc_info()[0]
            logging.error(f"Error: {e}")


def get_first_common_transaction_date(
    snowflake_analytics_connection: str,
    analytics_schema: str,
    merchant_guid: str,
    account_guids: List[str],
) -> datetime.date:
    production_engine = SnowflakeHook(
        snowflake_analytics_connection
    ).get_sqlalchemy_engine()

    with production_engine.begin() as tx:
        first_common_transaction_statement = first_common_transaction_date_query(
            snowflake_analytics_connection,
            analytics_schema,
            merchant_guid,
            account_guids,
        )

        row = tx.execute(first_common_transaction_statement).first()

        return row["first_common_transaction_date"]


def get_account_last_transaction_date(
    snowflake_analytics_connection: str,
    analytics_schema: str,
    merchant_guid: str,
    account_guids: List[str],
) -> Dict[str, str]:
    production_engine = SnowflakeHook(
        snowflake_analytics_connection
    ).get_sqlalchemy_engine()

    with production_engine.begin() as tx:
        last_transaction_statement = last_transaction_date_query(
            snowflake_analytics_connection,
            analytics_schema,
            merchant_guid,
            account_guids,
        )

        row = tx.execute(last_transaction_statement).first()

        last_transactions = json.loads(row["last_transaction_dates"])

        return last_transactions


def do_multi_account_projection(
    merchant_guid: str,
    account_guids: List[str],
    cash_flow_df: pd.DataFrame,
    projection_id: str,
    snowflake_zetatango_connection: str,
    snowflake_analytics_connection: str,
    analytics_schema: str,
    zetatango_schema: str,
) -> None:
    auto_arima_parameters = AutoArimaParameters()
    arima_projection_parameters = ArimaProjectionParameters()
    cash_flow_projection_parameters = CashFlowProjectionParameters()

    logging.info(
        f"Generating projections for {account_guids} for merchant {merchant_guid}"
    )

    transactions_info = get_account_last_transaction_date(
        snowflake_analytics_connection,
        analytics_schema,
        merchant_guid,
        account_guids,
    )

    logging.info(f"Last transactions: {transactions_info}")

    details = {
        "id": projection_id,
        "version": cash_flow_projection_parameters.version,
        "auto_arima_params": attr.asdict(auto_arima_parameters),
        "arima_projection_params": attr.asdict(arima_projection_parameters),
        "last_transaction_dates": dict(
            sorted(transactions_info.items(), key=lambda item: item[0])
        ),
    }

    logging.info(f"Details: {details}")

    parameters_to_hash: Dict[str, Dict[str, Any]] = {
        "auto_arima_params": cast(Dict[str, Any], details["auto_arima_params"]),
        "arima_projection_params": cast(
            Dict[str, Any], details["arima_projection_params"]
        ),
        "cash_flow_projection_parameters": cast(Dict[str, Any], details["version"]),
        "last_transaction_dates": cast(
            Dict[str, str], details["last_transaction_dates"]
        ),
    }

    logging.info(f"Params to hash (before): {parameters_to_hash}")

    parameters_to_hash["auto_arima_params"].pop("random_state", None)

    logging.info(f"Params to hash (after): {parameters_to_hash}")

    if skip_multi_account_projection(
        merchant_guid,
        account_guids,
        snowflake_zetatango_connection,
        zetatango_schema,
        parameters_to_hash,
    ):
        return

    if apply_pre_projection_guardrails(cash_flow_df, arima_projection_parameters):
        logging.warning(
            f"❌ Pre projection guardrail for {merchant_guid} - {account_guids} applied"
        )

        store_multi_account_projection(
            merchant_guid,
            account_guids,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    projection_df = calculate_projection(
        cash_flow_df,
        auto_arima_parameters,
        arima_projection_parameters,
    )

    if apply_post_projection_guardrail_closing_balance(cash_flow_df, projection_df):
        logging.warning(
            f"❌ Post projection guardrail on closing balance for {merchant_guid} - {account_guids} applied"
        )

        store_multi_account_projection(
            merchant_guid,
            account_guids,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    if guardrail_linear_regression(cash_flow_df, projection_df):
        logging.warning(
            f"❌ Post projection guardrail on linear regression for {merchant_guid} - {account_guids} applied"
        )

        store_multi_account_projection(
            merchant_guid,
            account_guids,
            cash_flow_df,
            parameters_to_hash,
            details,
            snowflake_zetatango_connection,
            zetatango_schema,
        )

        return

    projection_df.index = pd.to_datetime(projection_df.index).astype(str)
    details["data"] = projection_df.to_dict("index")

    store_multi_account_projection(
        merchant_guid,
        account_guids,
        cash_flow_df,
        parameters_to_hash,
        details,
        snowflake_zetatango_connection,
        zetatango_schema,
    )


def format_cash_flow_into_df(
    cash_flow_result: RowProxy, first_transaction_date: datetime.date
) -> pd.DataFrame:
    df = pd.DataFrame.from_dict(
        json.loads(cash_flow_result["daily_cash_flow"]), orient="index"
    )

    df.index = pd.to_datetime(df.index)

    full_index = pd.date_range(start=first_transaction_date, end=df.index.max())
    df = df.reindex(full_index)

    df.index.rename(inplace=True, name="DateTime")
    df.sort_index(inplace=True)

    for column in ["credits", "debits"]:
        df[column].fillna(0, inplace=True)

    df["balance"].ffill(inplace=True)

    print(f"Cash flow: {df}")

    return df


def generate_projections(
    snowflake_zetatango_connection: str,
    snowflake_analytics_connection: str,
    num_threads: int,
    task_instance_key_str: str,
    ts_nodash: str,
    analytics_schema: str,
    zetatango_schema: str,
    dag_run: DagRun,
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
        )

        projection_id: str = sha256(
            f"{task_instance_key_str}_{ts_nodash}".encode("utf-8")
        ).hexdigest()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for row in tx.execute(statement).fetchall():
                if "account_ids" in dag_run.conf:
                    merchant_guid = dag_run.conf["merchant_guid"]
                    account_guids = dag_run.conf["account_ids"]

                    if (
                        row["merchant_guid"] != merchant_guid
                        or row["account_guid"] not in account_guids
                    ):
                        continue

                df = pd.DataFrame.from_dict(
                    json.loads(row["daily_cash_flow"]), orient="index"
                )

                df.index = pd.to_datetime(df.index)

                full_index = pd.date_range(df.index.min(), df.index.max())
                df = df.reindex(full_index)

                df.index.rename(inplace=True, name="DateTime")
                df.sort_index(inplace=True)

                for column in ["credits", "debits"]:
                    df[column].fillna(0, inplace=True)
                df["balance"].ffill(inplace=True)

                try:
                    executor.submit(
                        do_projection,
                        row["merchant_guid"],
                        row["account_guid"],
                        df,
                        projection_id,
                        existing_projections_df,
                        snowflake_zetatango_connection,
                        zetatango_schema,
                    )
                except:
                    e = sys.exc_info()[0]
                    logging.error(f"Error: {e}")


def create_dag() -> DAG:
    with DAG(
        "cash_flow_projection",
        max_active_runs=10,
        schedule_interval=None,
        start_date=pendulum.datetime(
            2020, 8, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        on_failure_callback=slack_dag("slack_data_alerts"),
        default_args={"retries": 5, "retry_delay": timedelta(minutes=2)},
    ) as dag:
        dag << PythonOperator(
            task_id="generate_projections",
            python_callable=generate_projections,
            provide_context=True,
            op_kwargs={
                "snowflake_zetatango_connection": "snowflake_zetatango_production",
                "snowflake_analytics_connection": "airflow_production",
                "num_threads": 10,
                "analytics_schema": "DBT_ARIO",
                "zetatango_schema": "CORE_PRODUCTION",
            },
        ) >> PythonOperator(
            task_id="generate_multi_projections",
            python_callable=generate_multi_projections,
            provide_context=True,
            op_kwargs={
                "snowflake_zetatango_connection": "snowflake_zetatango_production",
                "snowflake_analytics_connection": "airflow_production",
                "analytics_schema": "DBT_ARIO",
                "zetatango_schema": "CORE_PRODUCTION",
            },
        ) >> DbtOperator(
            task_id="dbt_run_generate_projections",
            execution_timeout=timedelta(hours=1),
            action=DbtAction.run,
            models=(
                "fct_daily_bank_account_projection fct_weekly_bank_account_projection "
                "fct_monthly_bank_account_projection"
            ),
        ) >> PythonOperator(
            task_id="notify_subscribers",
            python_callable=notify_subscribers,
            provide_context=True,
            op_kwargs={
                "rabbit_url": Variable.get("CLOUDAMQP_URL"),
                "exchange_label": Variable.get("CLOUDAMQP_EXCHANGE"),
                "topic": Variable.get("CLOUDAMQP_TOPIC_CASH_FLOW_PROJECTION"),
            },
        )

        return dag


if __name__ == "__main__":
    from helpers.snowflake_hook import test_get_sqlalchemy_engine

    # Monkeypatch the get engine function to return the right engine depending on the connection string
    SnowflakeHook.get_sqlalchemy_engine = test_get_sqlalchemy_engine

    # Monkeypatch the AWS hack so we can set AWS creds in the environment
    hack_clear_aws_keys = lambda: None  # noqa

    #     copy_transactions(
    #         "snowflake_zetatango_production", "CORE_STAGING", "ario-documents-staging", 1
    #     )
    #     generate_projections(
    #         "snowflake_zetatango_production",
    #         "snowflake_analytics_connection",
    #         1,
    #         "task_id",
    #         "task_ts",
    #         "DBT_ARIO",
    #         "CORE_STAGING",
    #     )
#
#     generate_multi_projections(
#         "snowflake_zetatango_production",
#         "snowflake_analytics_connection",
#         "task_id",
#         "task_ts",
#         "DBT_ARIO",
#         "CORE_STAGING",
#         "m_u3QGuwJPKn3Z9PhX",
#         [
#             "2f896acc-28dc-4f73-b180-ef8bba92b1dc",
#             "e30202b7-7ac4-4823-6b24-08d80cd4bf89",
#             "701e48d0-44d1-4996-80df-0f3d9879ac1e"
#         ],
#         None,
#     )
else:
    globals()["cash_flow_projection"] = create_dag()
