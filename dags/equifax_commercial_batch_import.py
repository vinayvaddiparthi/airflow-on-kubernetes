from datetime import timedelta
from typing import Any
from equifax_extras.commercial_batch_data_type import dt_comm, dt_tcap
import boto3
import pandas as pd
import pendulum
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator

bucket = "tc-datalake"
commercial_prefix = "equifax_offline_batch/commercial/output/"

aws_hook = AwsHook(aws_conn_id="s3_equifax")
aws_credentials = aws_hook.get_credentials()
ssh_hook = SSHHook(ssh_conn_id="equifax_sftp")


def get_month_tag(ds: str) -> str:
    # Calculate import_month based on dag scheduled runtime
    then = pendulum.from_format(ds, "%Y-%m-%d")
    if then.month % 2 == 1:
        scheduled = then.add(months=-1)
    else:
        scheduled = then.add(months=-2)
    return f"{scheduled.year}{str(scheduled.month).zfill(2)}"


def commercial_batch_import(ds: str, **context: Any) -> None:
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )

    month_folder = f"{commercial_prefix}{get_month_tag(ds)}/"
    objects = client.list_objects(Bucket=bucket, Prefix=month_folder, Delimiter="/")
    for content in objects["Contents"]:
        file_name = content["Key"].split("/")[-1]
        if file_name and file_name.endswith("out1"):
            print(f"Processing file: {content['Key']}")
            obj = client.get_object(Bucket=bucket, Key=content["Key"])

            # decide upload file_name and target source table_name
            if "risk" in file_name.lower():
                print("comm")
                dt = dt_comm
                table_name = "equifax.public.equifax_comm"
            else:
                print("tcap")
                dt = dt_tcap
                table_name = "equifax.public.equifax_tcap"
            df = pd.read_csv(
                obj["Body"],
                delimiter=",",
                encoding="iso-8859-1",
                quotechar='"',
                dtype=dt,
            )

            # add 2 more extra identifiers to the record
            df["imported_file_name"] = file_name
            df["import_month"] = get_month_tag(ds)

            # generate csv file
            df.to_csv(f"{table_name}.csv", index=False, sep="\t")

            # upload converted csv to S3
            with open(f"{table_name}.csv", "rb") as file:
                print(f"Upload converted csv to S3: {file_name}")
                # upload converted csv to the same folder of output file
                path = content["Key"].replace(file_name, f"{table_name}.csv")
                client.upload_fileobj(file, bucket, path)

            # load csv to target source table in snowflake
            with SnowflakeHook(
                "airflow_production"
            ).get_sqlalchemy_engine().begin() as conn:
                sql_copy_stage = f"""COPY INTO {table_name}
                                     FROM S3://{bucket}/{path}
                                     CREDENTIALS = (
                                        aws_key_id='{aws_credentials.access_key}',
                                        aws_secret_key='{aws_credentials.secret_key}')
                                     FILE_FORMAT=(field_delimiter='\t', skip_header=1)"""
                cs = conn.cursor()
                cs.execute(sql_copy_stage)
                conn.commit()


with DAG(
    "equifax_commercial_output_dag",
    max_active_runs=1,
    schedule_interval="@monthly",
    start_date=pendulum.datetime(
        2020, 7, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
    default_args={"retries": 3, "retry_delay": timedelta(minutes=30)},
) as dag:
    dag << PythonOperator(
        task_id="commercial_batch_import",
        python_callable=commercial_batch_import,
        provide_context=True,
        dag=dag,
    )
