import datetime
from typing import Dict

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, text, column, and_, func, TIMESTAMP, cast, VARCHAR
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select


def ctas_to_glue(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    try:
        selectable: Select = sobject["selectable"]["callable"](
            table=sobject_name,
            engine=engine,
            **(sobject["selectable"].get("kwargs", {})),
        )
    except KeyError:
        selectable: Select = Select(
            columns=[text("*")],
            from_obj=text(f'"{sfdc_instance}"."salesforce"."{sobject_name}"'),
        )

    with engine.begin() as tx:
        tx.execute(
            f"""
        CREATE TABLE IF NOT EXISTS "glue"."{sfdc_instance}"."{sobject_name}" AS {selectable}
        WITH NO DATA
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                Select(
                    columns=[func.max(column(last_modified_field))],
                    from_obj=text(f'"glue"."{sfdc_instance}"."{sobject_name}"'),
                )
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        range_limited_selectable = selectable.where(
            column(last_modified_field) > cast(text(":max_date"), TIMESTAMP)
        )

        stmt = text(
            f'INSERT INTO "glue"."{sfdc_instance}"."{sobject_name}" {range_limited_selectable}'
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def ctas_to_snowflake(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    selectable: Select = Select(
        [text("*")], from_obj=text(f'"glue"."{sfdc_instance}"."{sobject_name}"')
    )

    with engine.begin() as tx:
        tx.execute(
            f"""
        CREATE TABLE IF NOT EXISTS "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" AS {selectable}
        WITH NO DATA
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                Select(
                    func.max(column(last_modified_field)),
                    from_obj=text(
                        '"sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}"'
                    ),
                )
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        cols_ = tx.execute(
            Select(
                [column("column_name"), column("data_type")],
                from_obj=text('"information_schema"."columns"'),
            )
            .where(column("table_schema") == text(f"'{sfdc_instance}'"))
            .where(column("table_name") == text(f"'{sobject_name}'"))
        ).fetchall()

        processed_columns = []
        for col_ in cols_:
            if col_[1].lower() == "varchar":
                processed_columns.append(
                    cast(column(col_[0]), VARCHAR(6291456)).label(col_[0])
                )
            else:
                processed_columns.append(column(col_[0]))

        selectable: Select = Select(
            processed_columns,
            from_obj=text(f'"glue"."{sfdc_instance}"."{sobject_name}"'),
        ).where(text(last_modified_field) > cast(text(":max_date"), TIMESTAMP))

        stmt = text(
            f'INSERT INTO "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" {selectable}'
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def create_sf_summary_table(conn: str, sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine: Engine = SnowflakeHook(snowflake_conn_id=conn).get_sqlalchemy_engine()

    with engine.begin() as tx:
        tx.execute(
            f"""
        CREATE OR REPLACE TABLE "SALESFORCE"."{sfdc_instance.upper()}"."{sobject_name.upper()}"
        as select distinct t0.* from "SALESFORCE"."{sfdc_instance.upper()}_RAW"."{sobject_name.upper()}" t0
        join (
            select id, max({last_modified_field}) as max_date
            from "SALESFORCE"."{sfdc_instance.upper()}_RAW"."{sobject_name.upper()}"
            group by id
            ) t1 on t0.id = t1.id and t0.{last_modified_field} = t1.max_date
        """
        ).fetchall()
