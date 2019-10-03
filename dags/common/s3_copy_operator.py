import traceback

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class S3CopyOperator(BaseOperator):
    ui_color = "#f5edff"
    ui_fgcolor = "#5e6266"

    def __init__(
            self,
            task_id: str,
            schema: str,
            table: str,
            path: str,
            delimiter: str,
            postgres_conn_id: str,
            s3_conn_id: str,
            *args,
            **kwargs,
    ):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.schema = schema
        self.table = table
        self.path = path
        self.delimiter = delimiter
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.aws_hook = AwsHook(aws_conn_id=s3_conn_id)

    def execute(self, context):
        conn = self.postgres_hook.get_conn()
        aws_credentials = self.aws_hook.get_credentials()

        pre_steps = [
            f"""copy {self.table} from 's3://{self.path}'
                            access_key_id '{aws_credentials.access_key}'
                            secret_access_key '{aws_credentials.secret_key}'
                            csv
                            delimiter '{self.delimiter}'
                            ignoreblanklines
                            acceptinvchars
                            dateformat 'auto'
                            timeformat 'auto'"""
        ]
        with conn.cursor() as cur:
            try:
                for query in pre_steps:
                    cur.execute(query)
                    conn.commit()
            except Exception as e:
                traceback.print_exc()
                raise e
