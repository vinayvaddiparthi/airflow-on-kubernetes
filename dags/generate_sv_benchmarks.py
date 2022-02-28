import pendulum
from datetime import timedelta
import tempfile
import time

from sqlalchemy import create_engine
from utils.failure_callbacks import slack_task

import pandas as pd
from snowflake.connector.pandas_tools import pd_writer

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

from sqlalchemy import Table, MetaData, VARCHAR
from sqlalchemy.sql import select, func, text, literal_column, literal, join, column

from helpers.salesvolume_classfication import (
    categorize_transactions,
)


def _classify_transactions(
    snowflake_conn: str, database: str, schema: str, table: str
) -> None:

    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    metadata = MetaData()

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

    ranked_trx = (
        select(
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
        )
        # .where(transactions.c.merchant_guid == 'm_yCWW9JAVdvxC13xW')
        .order_by(transactions.c.account_guid, transactions.c.merchant_guid)
    )

    latest_trx_select = select([ranked_trx]).where(
        ranked_trx.c.ranked == 1
    )  # .limit(1000000)

    cashflow_lookup_select = select([cash_flow_lookup]).where(
        cash_flow_lookup.c.cash_flow_lookup_version_id == 120
    )

    with engine.begin() as conn:

        df_lookup = pd.read_sql(cashflow_lookup_select, con=conn)

        with open("dags/sql/benchmarking/create_table.sql", "r") as f:
            create_stmt = f.read()
            create_stmt = create_stmt.replace("{table}", qualified_table)

        conn.execute(create_stmt)

        conn.execute(
            text(f"create or replace stage {stage} file_format=(type=parquet)")
        )

        time1 = time.time()

        # dfs = pd.read_sql(latest_trx_select, con=conn, chunksize=500000)

        # for df_transactions in dfs:

        #     df_transactions[['predicted_category', 'is_nsd']] = df_transactions.apply(lookup_entry_by_description, args=(df_lookup,), axis=1, result_type='expand')

        #     df_transactions.drop(columns=['ranked'], inplace=True)

        #     # cast date column to date object
        #     df_transactions.date = pd.to_datetime(df_transactions.date)

        #     with tempfile.NamedTemporaryFile() as temp_file:

        #         df_transactions.to_parquet(
        #             temp_file.name, engine="fastparquet", compression="gzip",
        #         )

        #         conn.execute(
        #             text(f"put file://{temp_file.name} @analytics_development.dbt_reporting.bfueb parallel=4")
        #         )

        df_transactions = pd.read_sql(latest_trx_select, con=conn)

        time2 = time.time()

        # df_transactions[['predicted_category', 'is_nsd']] = df_transactions.apply(lookup_entry_by_description, args=(df_lookup,), axis=1, result_type='expand')
        df_transactions[["predicted_category", "is_nsd"]] = df_transactions.apply(
            categorize_transactions, args=(df_lookup,), axis=1, result_type="expand"
        )

        time3 = time.time()

        df_transactions.drop(columns=["ranked"], inplace=True)

        # cast date column to date object
        df_transactions.date = pd.to_datetime(df_transactions.date)

        with tempfile.NamedTemporaryFile() as temp_file:

            df_transactions.to_parquet(
                temp_file.name,
                engine="fastparquet",
                compression="gzip",
            )

            time4 = time.time()

            conn.execute(text(f"put file://{temp_file.name} @{stage} parallel=4"))

        time5 = time.time()

        with open("dags/sql/benchmarking/load_table.sql", "r") as f:
            copy_stmt = f.read()
            copy_stmt = copy_stmt.replace("{table}", qualified_table).replace(
                "{stage}", stage
            )

        conn.execute(copy_stmt)

        time6 = time.time()

        # time4 = time.time()

        # df_transactions.to_sql(name='fct_categorized_bank_transactions', con=conn, if_exists='replace', index=False, method=pd_writer)

        # print(f"Time to stage transactions: {time2 - time1} | Time to write to db: {time3 - time2}")

        print(
            f"Time to load transactions: {time2 - time1} | Time to categorize: {time3 - time2} | Time to parquet: {time4 - time3} | Time to put: {time5 - time4} | Time to load to db: {time6 - time5}"
        )


def create_dag() -> DAG:
    with DAG(
        dag_id="generate_sv_benchmarks",
        max_active_runs=1,
        schedule_interval="0 4 * * *",
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": slack_task("slack_data_alerts"),
        },
        catchup=False,
        start_date=pendulum.datetime(
            2022, 2, 28, tzinfo=pendulum.timezone("America/Toronto")
        ),
    ) as dag:

        is_prod = Variable.get(key="environment") == "production"

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

        perform_aggregations = SnowflakeOperator(
            task_id="perform_aggregations",
            sql="sql/benchmarking/weekly_sales_volume_benchmarking.sql",
            params={
                "trx_table": "dbt_ario.fct_categorized_bank_transactions",
                "merchant_table": "dbt_ario.dim_merchant",
                "sv_table": "fct_sales_volume_industry_benchmarks",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema=f"dbt_reporting",
            snowflake_conn_id="snowflake_production",
            dag=dag,
        )

        dag << classify_transactions >> perform_aggregations

        return dag


if __name__ == "__main__":

    import os
    from unittest.mock import patch
    from snowflake.sqlalchemy import URL

    with patch(
        "generate_sv_benchmarks.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            URL(
                account="thinkingcapital.ca-central-1.aws",
                user=os.environ["SNOWFLAKE_USERNAME"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                database="analytics_development",
                schema="dbt_reporting",
                role="dbt_development",
            )
        ),
    ) as mock_engine, open(
        "dags/sql/benchmarking/weekly_sales_volume_benchmarking.sql", "r"
    ) as f:

        time1 = time.time()

        _classify_transactions(
            "abc", schema="dbt_ario", database="analytics_development"
        )

        time2 = time.time()

        query = f.read()

        params = {
            "trx_table": "fct_categorized_bank_transactions_v2",
            "merchant_table": "dbt_ario.dim_merchant",
            "sv_table": "fct_sales_volume_industry_benchmarks",
        }

        for param in params:
            query = query.replace(f"{{{{ params.{param} }}}}", f"{params[param]}")

        df = pd.read_sql_query(query, con=mock_engine.return_value)

        time3 = time.time()

        print(f"Classify: {time2 - time1} | Aggregate: {time3 - time2}")

else:
    globals()["sv_aggregates"] = create_dag()
