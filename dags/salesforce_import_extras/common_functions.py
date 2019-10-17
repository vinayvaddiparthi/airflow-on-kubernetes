import datetime
from typing import Dict

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, text, column, and_
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select


def ctas_to_glue(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    try:
        selectable = sobject["selectable"]["callable"](
            table=sobject_name,
            engine=engine,
            **(sobject["selectable"].get("kwargs", {})),
        ).__str__()
    except KeyError:
        selectable = f'select * from "{sfdc_instance}"."salesforce"."{sobject_name}"'

    with engine.begin() as tx:
        tx.execute(
            f"""
        create table if not exists "glue"."{sfdc_instance}"."{sobject_name}" as {selectable}
        with no data
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                f'select max({last_modified_field}) from "glue"."{sfdc_instance}"."{sobject_name}"'
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        stmt = text(
            f"""
        insert into "glue"."{sfdc_instance}"."{sobject_name}" {selectable}
        where {last_modified_field} > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def ctas_to_snowflake(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    with engine.begin() as tx:
        tx.execute(
            f"""
        create table if not exists "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" as select *
        from "glue"."{sfdc_instance}"."{sobject_name}"
        with no data
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                f'select max({last_modified_field}) from "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}"'
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        cols_ = tx.execute(
            Select(
                [column("column_name"), column("data_type")],
                from_obj=text('"information_schema"."columns"'),
                whereclause=and_(
                    column("table_schema") == text(f"'{sfdc_instance}'"),
                    column("table_name") == text(f"'{sobject_name}'"),
                ),
            )
        ).fetchall()

        columns_to_cast = []
        for col_ in cols_:
            if col_[1].lower() == "varchar":
                columns_to_cast.append(f'CAST("{col_[0]}" AS VARCHAR(6291456)) "{col_[0]}"')
            else:
                columns_to_cast.append(f'"{col_[0]}"')

        stmt = text(
            f"""
        insert into "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" select {", ".join(columns_to_cast)}
        from "glue"."{sfdc_instance}"."{sobject_name}"
        where {last_modified_field} > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def create_sf_summary_table(conn: str, sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]
    last_modified_field = sobject.get("last_modified_field", "systemmodstamp")

    engine: Engine = SnowflakeHook(snowflake_conn_id=conn).get_sqlalchemy_engine()

    with engine.begin() as tx:
        tx.execute(
            f"""
        create or replace table salesforce.{sfdc_instance}.{sobject_name}
        as select distinct t0.* from salesforce.{sfdc_instance}_raw.{sobject_name} t0
        join (
            select id, max({last_modified_field}) as max_date
            from salesforce.{sfdc_instance}_raw.{sobject_name}
            group by id
            ) t1 on t0.id = t1.id and t0.{last_modified_field} = t1.max_date
        """
        ).fetchall()
