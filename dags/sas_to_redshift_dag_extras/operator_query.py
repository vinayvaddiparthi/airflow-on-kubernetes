from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class QueryOperator(BaseOperator):
    ui_color = "#ebf5ff"
    ui_fgcolor = "#5e6266"

    def __init__(
        self,
        task_id: str,
        sql: list,
        postgres_conn_id: str,
        callable=None,
        *args,
        **kwargs
    ):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.sql = sql
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.callable = callable

    def execute(self, context):
        result = self.postgres_hook.get_first(self.sql)
        error = callable(result)
        if error:
            raise error
