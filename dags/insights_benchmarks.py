import pendulum
from datetime import timedelta
import tempfile

import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import select, func, text
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

from utils.failure_callbacks import slack_task
from helpers.salesvolume_classfication import (
    categorize_transactions,
)


def _classify_transactions(
    snowflake_conn: str, database: str, schema: str, table: str
) -> None:

    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine({"echo": True})
    metadata = MetaData()

    chunk_size = 100000

    stage = f"{database}.{schema}.categorized_transactions_stage"
    qualified_table = f"{database}.{schema}.{table}"

    transactions = Table(
        "fct_bank_account_transaction",
        metadata,
        autoload_with=engine,
        schema=schema,
        snowflake_database=database,
    )

    cash_flow_lookup = Table(
        "fct_cash_flow_lookup_entries",
        metadata,
        autoload_with=engine,
        schema=schema,
        snowflake_database=database,
    )

    ranked_trx = select(
        [
            transactions,
            func.rank()
            .over(
                partition_by=[
                    transactions.c.merchant_guid,
                    transactions.c.account_guid,
                ],
                order_by=transactions.c.batch_timestamp.desc(),
            )
            .label("ranked"),
        ]
    ).order_by(transactions.c.account_guid, transactions.c.merchant_guid)

    latest_trx_select = select([ranked_trx]).where(ranked_trx.c.ranked == 1)

    cashflow_lookup_select = select([cash_flow_lookup]).where(
        cash_flow_lookup.c.cash_flow_lookup_version_id == 120
    )

    with engine.begin() as conn:

        df_lookup = pd.read_sql(cashflow_lookup_select, con=conn)

        df_precise_entries = df_lookup[df_lookup["has_match"]]

        df_inprecise_entries = df_lookup[~df_lookup["has_match"]]

        with open("dags/sql/benchmarking/create_categorized_table.sql", "r") as f:
            create_stmt = f.read()
            create_stmt = create_stmt.replace("{table}", qualified_table)

        for stmt in create_stmt.split(";"):
            conn.execute(stmt)

        conn.execute(
            text(f"create or replace stage {stage} file_format=(type=parquet)")
        )

        dfs = pd.read_sql(latest_trx_select, con=conn, chunksize=chunk_size)

        for df_transactions in dfs:

            df_transactions[["predicted_category", "is_nsd"]] = df_transactions.apply(
                categorize_transactions,
                args=(df_precise_entries, df_inprecise_entries),
                axis=1,
                result_type="expand",
            )

            df_transactions.drop(columns=["ranked"], inplace=True)

            # cast date column to date object
            df_transactions.date = pd.to_datetime(df_transactions.date)

            with tempfile.NamedTemporaryFile() as temp_file:

                df_transactions.to_parquet(
                    temp_file.name,
                    engine="fastparquet",
                    compression="gzip",
                )

                conn.execute(text(f"put file://{temp_file.name} @{stage} parallel=4"))

        with open("dags/sql/benchmarking/load_categorized_table.sql", "r") as f:
            copy_stmt = f.read()
            copy_stmt = copy_stmt.replace("{table}", qualified_table).replace(
                "{stage}", stage
            )

        conn.execute(copy_stmt)


def create_dag() -> DAG:
    with DAG(
        dag_id="insights_benchmarks",
        max_active_runs=1,
        schedule_interval="0 8 * * 0",
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": slack_task("slack_data_alerts"),
        },
        catchup=False,
        start_date=pendulum.datetime(
            2022, 3, 8, tzinfo=pendulum.timezone("America/Toronto")
        ),
    ) as dag, open(
        "dags/sql/benchmarking/weekly_sales_volume_benchmarking.sql"
    ) as sv, open(
        "dags/sql/benchmarking/weekly_cashflow_benchmarking.sql"
    ) as cf:

        is_prod = Variable.get(key="environment") == "production"

        sv_queries = [query.strip("\n") for query in sv.read().split(";")]

        cf_queries = [query.strip("\n") for query in cf.read().split(";")]

        classify_transactions = PythonOperator(
            task_id="classify_transactions",
            python_callable=_classify_transactions,
            op_kwargs={
                "snowflake_conn": "snowflake_dbt",
                "database": f"{'analytics_production' if is_prod else 'analytics_development'}",
                "schema": "dbt_ario",
                "table": "fct_categorized_bank_transactions",
            },
            dag=dag,
        )

        sales_volume_benchmarks = SnowflakeOperator(
            task_id="generate_sv_benchmarks",
            sql=sv_queries,
            params={
                "trx_table": "dbt_ario.fct_categorized_bank_transactions",
                "merchant_table": "dbt_ario.dim_merchant",
                "sv_table": "dbt_reporting.fct_sales_volume_industry_benchmarks",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema="dbt_reporting",
            snowflake_conn_id="snowflake_production",
            dag=dag,
        )

        cashflow_benchmarks = SnowflakeOperator(
            task_id="generate_cf_benchmarks",
            sql=cf_queries,
            params={
                "trx_table": "dbt_ario.fct_weekly_bank_account_balance",
                "merchant_table": "dbt_ario.dim_merchant",
                "sv_table": "dbt_reporting.fct_cashflow_industry_benchmarks",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema="dbt_reporting",
            snowflake_conn_id="snowflake_production",
            dag=dag,
        )

        classify_transactions >> sales_volume_benchmarks

        cashflow_benchmarks

        return dag


globals()["sv_aggregates"] = create_dag()