import datetime
from typing import Set, Optional

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine, select, text, column, and_
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select, TableClause, ClauseElement


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def format_wide_table_select(
    table_name: str,
    engine: Engine,
    excluded_fields: Optional[Set[str]] = None,
    conditions: Optional[ClauseElement] = None,
):
    excluded_fields = excluded_fields or set()

    with engine.begin() as tx:
        stmt = Select(
            columns=[column("column_name")],
            from_obj=text("information_schema.columns"),
            whereclause=text(f"table_name = '{table_name}'"),
        )

        column_chunks = [
            x
            for x in chunks(
                [
                    column(x[0])
                    for x in tx.execute(stmt)
                    if x[0] not in {"id"}.union(excluded_fields)
                ],
                100,
            )
        ]

        tables = [
            select(
                [column("id")] + cols_,
                from_obj=TableClause(table_name, *([column("id")] + cols_)),
                whereclause=conditions,
            )
            for cols_ in column_chunks
        ]

        for i, table in enumerate(tables):
            table.schema = "salesforce"
            tables[i] = table.alias(f"t{i}")

        stmt = tables[0]

        for i in range(1, len(tables)):
            stmt = stmt.join(tables[i], tables[0].c.id == tables[i].c.id)

        stmt = select(
            [tables[0].c.id]
            + [
                col
                for table_ in tables
                for col in table_.c
                if col.name not in {"id"}.union(excluded_fields)
            ],
            from_obj=stmt,
        )

        return stmt


# wide_tables = [
#     {"name": "opportunity", "condition": None},
#     {"name": "pricing__c", "condition": None},
# ]


def ctas_to_glue(sfdc_instance: str, sobject: str):
    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    with engine.begin() as tx:
        tx.execute(
            f"""
        create table if not exists "glue"."{sfdc_instance}".{sobject} as select * from "{sfdc_instance}"."salesforce"."{sobject}"
        with no data
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                f'select max(systemmodstamp) from "glue"."{sfdc_instance}"."{sobject}"'
            ).fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        stmt = text(
            f"""
        insert into "glue"."{sfdc_instance}"."{sobject}" select * from "{sfdc_instance}"."salesforce"."{sobject}"
        where systemmodstamp > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def ctas_to_snowflake(sfdc_instance: str, sobject: str):
    engine = create_engine(
        f"presto://presto-production-internal.presto.svc:8080/{sfdc_instance}"
    )

    with engine.begin() as tx:
        tx.execute(
            f"""
        create table if not exists "sf_salesforce"."{sfdc_instance}"."{sobject}" as select *
        from "glue"."{sfdc_instance}"."{sobject}"
        with no data
        """
        ).fetchall()

        try:
            max_date = tx.execute(
                f'select max(systemmodstamp) from "sf_salesforce"."{sfdc_instance}"."{sobject}"'
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
                    column("table_name") == text(f"'{sobject}'"),
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
        insert into "sf_salesforce"."{sfdc_instance}"."{sobject}" select {", ".join(cast_cols)}
        from "glue"."{sfdc_instance}"."{sobject}"
        where systemmodstamp > cast(:max_date as timestamp)
        """
        ).bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def create_snowflake_materialized_view(conn: str, sfdc_instance: str, sobject: str):
    engine: Engine = SnowflakeHook(conn).get_sqlalchemy_engine()

    with engine.begin() as tx:
        tx.execute(
            f"""
        create or replace materialized view "salesforce"."{sfdc_instance}_summary"."{sobject}"
        as select distinct * from "salesforce"."{sfdc_instance}"."{sobject}" t0
        join (
            select id, max(systemmodstamp) as max_date
            from "salesforce"."{sfdc_instance}"."{sobject}" t1
            group by id
            ) on "t0"."id" = "t1"."id" and "t0"."systemmodstamp" = "t1"."max_date"
        """
        ).fetchall()
