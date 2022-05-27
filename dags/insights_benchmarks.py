import os
import pendulum
from datetime import timedelta, datetime
import tempfile

import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import select, func, text
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

from utils.failure_callbacks import slack_task
from helpers.salesvolume_classfication import SalesClassification


def _classify_transactions(
    snowflake_conn: str, database: str, schema: str, table: str
) -> None:

    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    metadata = MetaData()

    svc = SalesClassification()

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

    merchant = Table(
        "dim_merchant",
        metadata,
        autoload_with=engine,
        schema=schema,
        snowflake_database=database,
    )

    industry_lookup = Table(
        "fct_industry_lookup_entries",
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

    cashflow_lookup_select = select([cash_flow_lookup])

    merchant_select = select([merchant.c.guid, merchant.c.industry])

    industry_lookup_select = select([industry_lookup])

    with engine.begin() as conn:

        df_lookup = pd.read_sql(cashflow_lookup_select, con=conn)

        df_lookup = df_lookup[
            df_lookup["cash_flow_lookup_version_id"]
            == df_lookup["cash_flow_lookup_version_id"].max()
        ]

        df_lookup.sort_values("position", ascending=True, inplace=True)

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

        df_merchant = pd.read_sql(merchant_select, con=conn)

        df_industry_lookup = pd.read_sql(industry_lookup_select, con=conn)

        df_industry_lookup = df_industry_lookup[
            df_industry_lookup["industry_lookup_version_id"]
            == df_industry_lookup["industry_lookup_version_id"].max()
        ]

        df_merchant_industry = df_merchant.merge(
            df_industry_lookup, how="left", left_on="industry", right_on="industry"
        )

        for df_transactions in dfs:

            # trim the dataframe to only include the merchants present in df_transactions
            df_merchant_industry_subset = df_merchant_industry[
                df_merchant_industry["guid"].isin(df_transactions["merchant_guid"])
            ]

            df_merchant_industry_subset.drop_duplicates(subset="guid", inplace=True)

            df_transactions[
                ["predicted_category", "is_nsd", "processed_credit"]
            ] = df_transactions.apply(
                svc.calculate_sales_volume,
                args=(
                    df_precise_entries,
                    df_inprecise_entries,
                    df_merchant_industry_subset,
                ),
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


def _upload_benchmarks(
    snowflake_conn: str,
    s3_conn: str,
    s3_bucket: str,
    database: str,
    schema: str,
    table: str,
    file: str,
) -> None:

    s3_hook = S3Hook(aws_conn_id=s3_conn)

    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine({"echo": True})
    metadata = MetaData()

    with engine.begin() as tx:

        tx.execute(f"use database {database}")
        tx.execute(f"use schema {schema}")

    benchmarks = Table(
        table,
        metadata,
        autoload_with=engine,
        schema=schema,
        snowflake_database=database,
    )

    benchmarks_select = select([benchmarks])

    with engine.begin() as conn:

        df_benchmarks = pd.read_sql(benchmarks_select, con=conn)

        # cast week as string to preserve the format during json conversion
        df_benchmarks["month"] = df_benchmarks["month"].astype(str)

        # fetching the most recent load to upload to s3
        df_benchmarks_latest = df_benchmarks[
            df_benchmarks["report_ts"] == df_benchmarks["report_ts"].max()
        ]

        df_benchmarks_latest.drop(columns=["report_ts"], inplace=True)

        df_benchmarks_latest = df_benchmarks_latest.sort_values(
            by=["macro_industry", "month"]
        )

    with tempfile.TemporaryDirectory() as temp_dir:

        today = f"{datetime.today():%Y_%m_%d}"
        file_name = f"{today}_{file}"
        file_path = os.path.join(temp_dir, file_name)

        df_benchmarks_latest.to_json(file_path, orient="records", indent=4)

        s3_hook.load_file(
            file_path,
            key=f"benchmarks/{file_name}",
            bucket_name=s3_bucket,
            replace=True,
        )


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
    ) as dag, open("dags/sql/benchmarking/sales_volume_benchmarks.sql") as sv, open(
        "dags/sql/benchmarking/cashflow_benchmarks.sql"
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
            snowflake_conn_id="snowflake_dbt",
            dag=dag,
        )

        upload_sv_benchmarks = PythonOperator(
            task_id="upload_sv_benchmarks",
            python_callable=_upload_benchmarks,
            op_kwargs={
                "snowflake_conn": "snowflake_dbt",
                "s3_conn": "s3_dataops",
                "s3_bucket": "tc-data-airflow-production",
                "database": f"{'analytics_production' if is_prod else 'analytics_development'}",
                "schema": "dbt_reporting",
                "table": "fct_sales_volume_industry_benchmarks",
                "file": "salesvolume_industry_benchmarks.json",
            },
            dag=dag,
        )

        cashflow_benchmarks = SnowflakeOperator(
            task_id="generate_cf_benchmarks",
            sql=cf_queries,
            params={
                "trx_table": "dbt_ario.fct_bank_account_transaction",
                "merchant_table": "dbt_ario.dim_merchant",
                "sv_table": "dbt_reporting.fct_cashflow_industry_benchmarks",
            },
            database=f"{'analytics_production' if is_prod else 'analytics_development'}",
            schema="dbt_reporting",
            snowflake_conn_id="snowflake_dbt",
            dag=dag,
        )

        upload_cf_benchmarks = PythonOperator(
            task_id="upload_cf_benchmarks",
            python_callable=_upload_benchmarks,
            op_kwargs={
                "snowflake_conn": "snowflake_dbt",
                "s3_conn": "s3_dataops",
                "s3_bucket": "tc-data-airflow-production",
                "database": f"{'analytics_production' if is_prod else 'analytics_development'}",
                "schema": "dbt_reporting",
                "table": "fct_cashflow_industry_benchmarks",
                "file": "cashflow_industry_benchmarks.json",
            },
            dag=dag,
        )

        classify_transactions >> sales_volume_benchmarks >> upload_sv_benchmarks

        cashflow_benchmarks >> upload_cf_benchmarks

        return dag


globals()["insights_benchmarks"] = create_dag()
