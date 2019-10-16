import datetime
from typing import Optional, Dict

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, text, column, and_
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select


def ctas_to_glue(sfdc_instance: str, sobject: Dict):
    sobject_name = sobject["name"]

    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    try:
        selectable = sobject["selectable"]["callable"](
            sobject_name=sobject_name,
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
                f'select max(systemmodstamp) from "glue"."{sfdc_instance}"."{sobject_name}"'
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        stmt = text(
            f"""
        insert into "glue"."{sfdc_instance}"."{sobject_name}" {selectable}
        where systemmodstamp > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def ctas_to_snowflake(sfdc_instance: str, sobject_name: str):
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
                f'select max(systemmodstamp) from "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}"'
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

        cast_cols = []
        for col_ in cols_:
            if col_[1].lower() == "varchar":
                cast_cols.append(f'CAST("{col_[0]}" AS VARCHAR(6291456)) "{col_[0]}"')
            else:
                cast_cols.append(f'"{col_[0]}"')

        stmt = text(
            f"""
        insert into "sf_salesforce"."{sfdc_instance}_raw"."{sobject_name}" select {", ".join(cast_cols)}
        from "glue"."{sfdc_instance}"."{sobject_name}"
        where systemmodstamp > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def create_sf_summary_table(conn: str, sfdc_instance: str, sobject_name: str):
    engine: Engine = SnowflakeHook(snowflake_conn_id=conn).get_sqlalchemy_engine()

    with engine.begin() as tx:
        tx.execute(
            f"""
        create or replace table salesforce.{sfdc_instance}.{sobject_name}
        as select distinct t0.* from salesforce.{sfdc_instance}_raw.{sobject_name} t0
        join (
            select id, max(systemmodstamp) as max_date
            from salesforce.{sfdc_instance}_raw.{sobject_name}
            group by id
            ) t1 on t0.id = t1.id and t0.systemmodstamp = t1.max_date
        """
        ).fetchall()
