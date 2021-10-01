"""
#### Description
This workflow processes the consumer response file from Equifax. Once the response file is downloaded, the encrypted
response file will be processed and uploaded to Snowflake.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

import pendulum
import logging
import boto3
import pandas as pd
import tempfile
from datetime import datetime
from typing import Dict, List, Any

from helpers.aws_hack import hack_clear_aws_keys
from utils.failure_callbacks import slack_dag, sensor_timeout
from utils.gpg import init_gnupg
from utils.reference_data import result_dict, date_columns, personal_info
from utils.equifax_helpers import get_import_month

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(
        2021, 1, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
    "retries": 0,
    "catchup": False,
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "tags": ["equifax"],
    "description": "A workflow to download and process the consumer batch response file from Equifax",
}

dag = DAG(
    dag_id="equifax_consumer_inbox",
    schedule_interval="0 12 * * *",
    default_args=default_args,
)
dag.doc_md = __doc__

snowflake_connection = "snowflake_production"
s3_connection = "s3_dataops"
S3_BUCKET = "tc-data-airflow-production"
aws_hook = AwsBaseHook(aws_conn_id=s3_connection, client_type="s3")
aws_credentials = aws_hook.get_credentials()
sftp_connection = "equifax_sftp"
CONSUMER_FILENAME = (
    "{{ ti.xcom_pull(task_ids='check_if_response_available', key='filename') }}"
)


def _check_if_file_downloaded() -> bool:
    return Variable.get("equifax_consumer_response_downloaded") != "True"


def _check_if_response_available(ti: TaskInstance, **_: None) -> bool:
    hook = SFTPHook(ftp_conn_id=sftp_connection)
    files = hook.list_directory(path="outbox/")
    if len(files) == 0:
        return False
    # can safely assume only 1 consumer file will be available every month as Equifax clears the directory after 7 days
    filename = [file for file in files if file.startswith("exthinkingpd.eqxcan.ds")]
    if len(filename) == 0:
        return False
    filename_list = filename[0].split(".")
    filename_list_no_file_type = filename_list[:-2]
    filename_no_file_type = ".".join(filename_list_no_file_type)
    logging.info(filename_no_file_type)
    ti.xcom_push(key="filename", value=filename_no_file_type)
    return True


def _mark_response_as_downloaded(context: Dict) -> None:
    Variable.set("equifax_consumer_response_downloaded", True)
    logging.info(context["task_instance"].log_url)
    logging.info("Response file successfully downloaded.")


def _decrypt_response_file(
    s3_conn: str, bucket_name: str, download_key: str, upload_key: str
) -> None:
    hook = S3Hook(aws_conn_id=s3_conn)
    filename = hook.download_file(key=download_key, bucket_name=bucket_name)
    with open(filename, "rb") as reader:
        gpg = init_gnupg()
        passphrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
        decrypted_message = gpg.decrypt_file(
            reader, always_trust=True, passphrase=passphrase
        )
    with open(filename, "wb") as writer:
        writer.write(decrypted_message.data)
        hook.load_file(
            filename=filename, key=upload_key, bucket_name=bucket_name, replace=True
        )


def get_s3_client() -> Any:
    hack_clear_aws_keys()
    return boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )


def upload_file_s3(file: Any, path: str, bucket: str) -> None:
    file.seek(0)
    try:
        client = get_s3_client()
        client.upload_file(
            file.name,
            bucket,
            path,
        )
    except:
        logging.error("Error when uploading file to s3")


def _generate_index_list(start_idx: int, column_lengths: Dict) -> List:
    """
    Returns a list of column indices given a starting index and a dictionary mapping column name to column length
    """
    result = [start_idx]
    for column in column_lengths:
        result.append(result[-1] + column_lengths[column])
    return result[:-1]


def _convert_line_csv(line: str, indices: list) -> str:
    parts = []
    x = zip(indices, indices[1:] + [None])
    for start_idx, end_idx in x:
        if end_idx in personal_info:
            parts.append("")
        else:
            parts.append(line[start_idx:end_idx].strip().replace(",", "\,"))
    return ",".join(parts)


def _convert_file(bucket_name: str, download_key: str, upload_key: str) -> None:
    """
    Convert decrypted response file (txt) to csv according to our field dictionary
    """
    client = get_s3_client()
    try:
        logging.info(f"Getting object {download_key} from {bucket_name}")
        file = client.get_object(Bucket=bucket_name, Key=download_key)
        logging.info(file)
        body = file["Body"].read()
        content = body.decode("ISO-8859-1")
        with tempfile.TemporaryFile(
            mode="w+", encoding="ISO-8859-1"
        ) as raw, tempfile.NamedTemporaryFile(
            mode="w+", encoding="ISO-8859-1"
        ) as formatted:
            raw.write(content)
            raw.seek(0)
            lines = []
            for line in raw.readlines():
                if (
                    not line.startswith("BHDR-EQUIFAX")
                    and not line.startswith("BTRL-EQUIFAX")
                    and line
                ):
                    indices = _generate_index_list(
                        start_idx=0, column_lengths=result_dict
                    )
                    lines.append(_convert_line_csv(line=line, indices=indices))
                    formatted.write(_convert_line_csv(line=line, indices=indices))
                    formatted.write("\n")
            upload_key_split = upload_key.split(".")
            upload_key_split.pop()
            upload_key_no_file_type = ".".join(upload_key_split)
            upload_file_s3(
                file=formatted, path=f"{upload_key_no_file_type}.csv", bucket=S3_BUCKET
            )
    except Exception as e:
        raise Exception(
            f"Unable to get object {download_key} from {bucket_name}: {e} or convert to csv"
        )


def _get_col_def(column: str, length: int, date_formatted: bool) -> str:
    if date_formatted and column in date_columns:
        return f"{column} date"
    return f"{column} varchar({length})"


def _insert_snowflake(
    table: str, download_key: str, date_formatted: bool = False
) -> None:
    d3: Dict[str, int] = result_dict
    column_datatypes = []
    column_names = []
    for col, length in d3.items():
        column_datatypes.append(
            _get_col_def(column=col, length=length, date_formatted=date_formatted)
        )
        column_names.append(col)
    with SnowflakeHook(
        snowflake_conn_id=snowflake_connection
    ).get_sqlalchemy_engine().begin() as snowflake:
        sql = f"create or replace table {table} ({','.join(column_datatypes)});"
        snowflake.execute(sql)

        copy = f"""
            COPY INTO {table} FROM {download_key} 
            CREDENTIALS = (
                aws_key_id='{aws_credentials.access_key}',
                aws_secret_key='{aws_credentials.secret_key}'
            )
            FILE_FORMAT = (
                field_delimiter=',',
                field_optionally_enclosed_by = '"'
                {', skip_header=1' if date_formatted else ''}
            )
        """
        snowflake.execute(copy)


def _insert_snowflake_raw(
    table_name_raw: str,
    table_name_raw_history: str,
    download_key: str,
    next_ds_nodash: str,
    **_: None,
) -> None:
    _insert_snowflake(table=table_name_raw, download_key=download_key)
    _insert_snowflake(
        table=f"{table_name_raw_history}_{get_import_month(next_ds_nodash)}",
        download_key=download_key,
    )


def _convert_date_format(value: str) -> Any:
    t = datetime.now()
    if value is not None and "-" not in value and not value.isspace():
        try:
            m = value[:2]
            d = value[2:4]
            y = value[4:]
            if int(y) == t.year % 100 and int(m) <= t.month or int(y) < t.year % 100:
                yy = f"20{y}"
            else:
                yy = f"19{y}"
            dt = datetime.strptime(f"{yy}-{m}-{d}", "%Y-%m-%d")
            return dt
        except Exception as e:
            logging.error(e)
    return None


def _fix_date_format(table_name_raw: str, upload_key: str) -> None:
    """
    Date format of listed field are SAS format,
    and they are not valid to be converted into datetime directly with snowflake to_date()
    Therefore, we fix the format string value to make them compatible.
    """
    with SnowflakeHook(
        snowflake_conn_id=snowflake_connection
    ).get_sqlalchemy_engine().begin() as snowflake:
        select = f"select * from {table_name_raw}"  # nosec
        result = snowflake.execute(select)

        df = pd.DataFrame(result.cursor.fetchall())
        df.columns = [des[0] for des in result.cursor.description]
        for key in date_columns:
            df[key] = df[key].apply(lambda x: _convert_date_format(x))

        with tempfile.NamedTemporaryFile(mode="w") as file_in:
            df.to_csv(file_in.name, index=False, sep=",")
            with open(file_in.name, "rb") as file:
                upload_file_s3(file=file, path=upload_key, bucket=S3_BUCKET)


def _insert_snowflake_stage(
    table_name: str,
    table_name_history: str,
    download_key: str,
    next_ds_nodash: str,
    **_: None,
) -> None:
    _insert_snowflake(table=table_name, download_key=download_key, date_formatted=True)
    _insert_snowflake(
        table=f"{table_name_history}_{get_import_month(next_ds_nodash)}",
        download_key=download_key,
        date_formatted=True,
    )


def _insert_snowflake_public(
    source_table: str, destination_table: str, next_ds_nodash: str, **_: None
) -> None:
    columns = [
        "import_month",
        "accountid",
        "contractid",
        "business_name",
        *result_dict.keys(),
    ]
    columns_string = ",".join(columns)
    logging.info(f"Inserting {len(columns)} columns to {destination_table}")

    sql = f"""
        insert into {destination_table}({columns_string})
        select
        '{get_import_month(next_ds_nodash)}' as import_month,
        null as accountid,
        null as contractid,
        null as business_name,
        staging.*
        from {source_table} as staging
    """
    with SnowflakeHook(
        snowflake_conn_id=snowflake_connection
    ).get_sqlalchemy_engine().begin() as snowflake:
        snowflake.execute(sql)


check_if_file_downloaded = ShortCircuitOperator(
    task_id="check_if_file_downloaded",
    python_callable=_check_if_file_downloaded,
    do_xcom_push=False,
    dag=dag,
)

check_if_response_available = ShortCircuitOperator(
    task_id="check_if_response_available",
    python_callable=_check_if_response_available,
    provide_context=True,
    dag=dag,
)

create_sftp_to_s3_job = SFTPToS3Operator(
    task_id="create_sftp_to_s3_job",
    sftp_conn_id=sftp_connection,
    sftp_path=f"outbox/{CONSUMER_FILENAME}.txt.pgp",
    s3_conn_id=s3_connection,
    s3_bucket=S3_BUCKET,
    s3_key=f"equifax/consumer/inbox/{CONSUMER_FILENAME}.txt.pgp",
    on_success_callback=_mark_response_as_downloaded,
    dag=dag,
)

decrypt_response_file = PythonOperator(
    task_id="decrypt_response_file",
    python_callable=_decrypt_response_file,
    op_kwargs={
        "s3_conn": s3_connection,
        "bucket_name": S3_BUCKET,
        "download_key": f"equifax/consumer/inbox/{CONSUMER_FILENAME}.txt.pgp",
        "upload_key": f"equifax/consumer/decrypted/{CONSUMER_FILENAME}.txt",
    },
    dag=dag,
)

is_decrypted_response_file_available = S3KeySensor(
    task_id="is_decrypted_response_file_available",
    bucket_key=f"s3://{S3_BUCKET}/equifax/consumer/decrypted/{CONSUMER_FILENAME}.txt",
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=sensor_timeout,
    dag=dag,
)

convert_file = PythonOperator(
    task_id="convert_file",
    python_callable=_convert_file,
    op_kwargs={
        "bucket_name": S3_BUCKET,
        "download_key": f"equifax/consumer/decrypted/{CONSUMER_FILENAME}.txt",
        "upload_key": f"equifax/consumer/csv/{CONSUMER_FILENAME}.csv",
    },
    dag=dag,
)

insert_snowflake_raw = PythonOperator(
    task_id="insert_snowflake_raw",
    python_callable=_insert_snowflake_raw,
    op_kwargs={
        "table_name_raw": "equifax.output.consumer_batch_raw",
        "table_name_raw_history": "equifax.output_history.consumer_batch_raw",
        "download_key": f"s3://{S3_BUCKET}/equifax/consumer/csv/{CONSUMER_FILENAME}.csv",
    },
    provide_context=True,
    dag=dag,
)

fix_date = PythonOperator(
    task_id="fix_date_format",
    python_callable=_fix_date_format,
    op_kwargs={
        "table_name_raw": "equifax.output.consumer_batch_raw",
        "upload_key": f"equifax/consumer/csv_date_format_fixed/{CONSUMER_FILENAME}_date_format_fixed.csv",
    },
    dag=dag,
)

insert_snowflake_stage = PythonOperator(
    task_id="insert_snowflake_stage",
    python_callable=_insert_snowflake_stage,
    op_kwargs={
        "table_name": "equifax.output.consumer_batch",
        "table_name_history": "equifax.output_history.consumer_batch",
        "download_key": f"s3://{S3_BUCKET}/equifax/consumer/csv_date_format_fixed/{CONSUMER_FILENAME}_date_format_fixed.csv",
    },
    provide_context=True,
    dag=dag,
)

insert_snowflake_public = PythonOperator(
    task_id="insert_snowflake_public",
    python_callable=_insert_snowflake_public,
    op_kwargs={
        "source_table": "equifax.output.consumer_batch",
        "destination_table": "equifax.public.consumer_batch",
    },
    provide_context=True,
    dag=dag,
)

(
    check_if_file_downloaded
    >> check_if_response_available
    >> create_sftp_to_s3_job
    >> decrypt_response_file
    >> is_decrypted_response_file_available
    >> convert_file
    >> insert_snowflake_raw
    >> fix_date
    >> insert_snowflake_stage
    >> insert_snowflake_public
)
