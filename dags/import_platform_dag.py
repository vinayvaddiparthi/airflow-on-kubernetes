from typing import Dict

import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text, and_, create_engine, cast, VARCHAR, column, JSON, func
from sqlalchemy.sql import Select
from airflow import DAG


PRESTO_ADDR = "presto://presto-production-internal.presto.svc:8080"


def format_load_as_json_query(catalog: str, schema: str, table: str) -> Select:
    """
    Takes a catalog, schema and table as input and returns a Select object
    that represents a query to load the data and transform it in JSON.

    :param catalog:
    :param schema:
    :param table:
    :return:
    """
    from_obj = text(f'"{catalog}"."{schema}"."{table}"')

    engine = create_engine(PRESTO_ADDR)

    column_names_selectable = Select(
        columns=[text("column_name")],
        from_obj=text(f'"{catalog}"."information_schema"."columns"'),
        whereclause=and_(
            text(f"\"table_catalog\" = '{catalog}'"),
            text(f"\"table_schema\" = '{schema}'"),
            text(f"\"table_name\" = '{table}'"),
        ),
    )

    with engine.begin() as tx:
        resultset = [x[0] for x in tx.execute(column_names_selectable).fetchall()]

    column_names = [f"'{column(x)}'" for x in resultset]
    casts = [f"{cast(column(x), VARCHAR)}" for x in resultset]
    names_array = text(f"ARRAY[{', '.join(column_names)}]")
    expr_array = text(f"ARRAY[{', '.join(casts)}]")

    json_fn = func.json_format(cast(func.map(names_array, expr_array), JSON))

    return Select(columns=[json_fn], from_obj=from_obj)


def load(src: Dict[str, str], dst: Dict[str, str]) -> None:
    """
    Extracts data from source table src and loads it into table dst in JSON format.

    :param src:
    :param dst:
    :return:
    """
    to_obj = text(f'"{dst["catalog"]}"."{dst["schema"]}"."{dst["table"]}__SWAP"')

    selectable = format_load_as_json_query(src["catalog"], src["schema"], src["table"])

    with create_engine(PRESTO_ADDR) as tx:
        tx.execute(f"CREATE TABLE IF NOT EXISTS {to_obj} AS {selectable}")


def snowflake_swap(src: Dict[str, str], dst: Dict[str, str]) -> None:
    """
    Performs a table swap on the destination table in Snowflake in a transaction.

    :param src:
    :param dst:
    :return:
    """
    from_obj = text(
        f'"{src["catalog"].upper()}"."{src["schema"].upper()}"."{src["table"].upper()}__SWAP"'
    )
    to_obj = text(
        f'"{dst["catalog"].upper()}"."{dst["schema"].upper()}"."{dst["table"].upper()}"'
    )

    with SnowflakeHook("snowflake_default").get_sqlalchemy_engine().begin() as tx:
        tx.execute(f"DROP TABLE IF EXISTS {to_obj}")
        tx.execute(f"ALTER TABLE IF EXISTS {from_obj} RENAME TO {to_obj}")


def _create_dag(
    dag_id: str,
    start_date: pendulum.datetime,
    presto_input_catalog: str,
    presto_output_catalog: str,
    snow_dbname: str,
    schema: str,
) -> DAG:
    stmt = Select(
        columns=[column("table_catalog"), column("table_schema"), column("table_name")],
        from_obj=text(f'"{presto_input_catalog}"."information_schema"."tables"'),
        whereclause=text(f"table_schema = '{schema}'"),
    )

    with create_engine(PRESTO_ADDR).begin() as tx:
        resultset = tx.execute(stmt).fetchall()

    with DAG(dag_id=dag_id, start_date=start_date, schedule_interval="@daily",) as dag:
        for catalog, schema, table in resultset:
            dag << PythonOperator(
                task_id=f"extract_load__{catalog}__{schema}__{table}",
                python_callable=load,
                op_kwargs={
                    "src": {"catalog": catalog, "schema": schema, "table": table,},
                    "dst": {
                        "catalog": presto_output_catalog,
                        "schema": schema,
                        "table": table,
                    },
                },
            ) >> PythonOperator(
                task_id=f"snowflake_swap__{catalog}__{schema}__{table}",
                python_callable=snowflake_swap,
                op_kwargs={
                    "src": {
                        "catalog": snow_dbname,
                        "schema": schema,
                        "table": f"{table}__SWAP",
                    },
                    "dst": {"catalog": snow_dbname, "schema": schema, "table": table,},
                },
            )

    return dag


globals()["import_kyc_staging"] = _create_dag(
    "import__kyc_staging",
    start_date=pendulum.datetime(
        2020, 2, 12, tzinfo=pendulum.timezone("America/Toronto"),
    ),
    presto_input_catalog="airflow_sandbox_pr",
    presto_output_catalog="sf_kyc_staging",
    snow_dbname="KYC_STAGING",
    schema="public",
)
