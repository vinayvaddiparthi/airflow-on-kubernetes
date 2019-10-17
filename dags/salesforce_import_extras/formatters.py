from typing import Optional

from sqlalchemy import column, text, select
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ClauseElement, Select, TableClause


def format_wide_table_select(
    sobject_name: str, engine: Engine, condition: Optional[ClauseElement] = None
):
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i : i + n]

    with engine.begin() as tx:
        stmt = Select(
            columns=[column("column_name")],
            from_obj=text("information_schema.columns"),
            whereclause=text(f"table_name = '{sobject_name}'"),
        )

        column_chunks = [
            x
            for x in chunks(
                [column(x[0]) for x in tx.execute(stmt) if x[0] != "id"], 256
            )
        ]

        tables = [
            select(
                [column("id")] + cols_,
                from_obj=TableClause(sobject_name, *([column("id")] + cols_)),
                whereclause=condition,
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
            + [col for table_ in tables for col in table_.c if col.name != "id"],
            from_obj=stmt,
        )

        return stmt
