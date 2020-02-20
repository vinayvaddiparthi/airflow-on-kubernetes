from sqlalchemy import column, text, create_engine
from sqlalchemy.sql import Select

from import_platform_dag import snowflake_swap, load

PRESTO_ADDR = "presto://localhost:8080"


def run(
    presto_input_catalog: str,
    presto_output_catalog: str,
    snow_dbname: str,
    schema: str,
):
    stmt = Select(
        columns=[column("table_catalog"), column("table_schema"), column("table_name")],
        from_obj=text(f'"{presto_input_catalog}"."information_schema"."tables"'),
        whereclause=text(f"table_schema = '{schema}'"),
    )

    with create_engine(PRESTO_ADDR).begin() as tx:
        resultset = tx.execute(stmt).fetchall()

    for catalog, schema, table in resultset:
        load(
            src={"catalog": catalog, "schema": schema, "table": table,},
            dst={"catalog": presto_output_catalog, "schema": schema, "table": table,},
        )

        snowflake_swap(
            src={"catalog": snow_dbname, "schema": schema, "table": f"{table}__SWAP",},
            dst={"catalog": snow_dbname, "schema": schema, "table": table,},
        )


if __name__ == "__main__":
    pass