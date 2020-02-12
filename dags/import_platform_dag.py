import pendulum
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text, and_, create_engine, cast, VARCHAR, column, JSON
from sqlalchemy.sql import Select
from airflow import DAG


PRESTO_ADDR = "presto://presto-production-internal.presto.svc:8080"


def format_json_conversion_query(catalog: str, schema: str, table: str):
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

    map_ = text(f"MAP({names_array}, {expr_array})")
    json_fn = text(f"JSON_FORMAT({cast(map_, JSON)})")

    print(f"{Select(columns=[json_fn], from_obj=from_obj)}")


def _create_dag(
    dag_id: str, start_date: pendulum.datetime, catalog: str, schema: str,
) -> DAG:
    stmt = Select(
        columns=[column("table_catalog"), column("table_schema"), column("table_name")],
        from_obj=text(f'"{catalog}"."information_schema"."tables"'),
        whereclause=text("table_schema") == schema,
    )

    with create_engine(PRESTO_ADDR).begin() as tx:
        resultset = tx.execute(stmt).fetchall()

    with DAG(dag_id=dag_id, start_date=start_date, schedule_interval="@daily",) as dag:
        for catalog, schema, table in resultset:
            dag << PythonOperator(
                task_id=f"{catalog}__{schema}__{table}",
                callable=format_json_conversion_query,
                op_kwargs={"catalog": catalog, "schema": schema, "table": table,},
            )

    return dag


globals()["import_cg_lms_generic"] = _create_dag(
    "import__cg_lms_prod__cg-lms",
    start_date=pendulum.datetime(
        2020, 2, 12, tzinfo=pendulum.timezone("America/Toronto"),
    ),
    catalog="cg_lms_prod",
    schema="cg-lms",
)
