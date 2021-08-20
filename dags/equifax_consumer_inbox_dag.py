"""
#### Description
This workflow processes the response file from Equifax. Currently, we manually rename and upload the .out1 file provided
by Risk to the S3 bucket [advanceit] tc-datalake/equifax_automated_batch/response/consumer/. Once the response file is
downloaded, the DAG will process the file into a CSV format and upload it to
[advanceit] tc-datalake/equifax_automated_batch/output/consumer/.
Then the CSV will be copied into Snowflake table EQUIFAX.PUBLIC.CONSUMER_BATCH as well as a history table.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

from datetime import datetime, timedelta
import logging
import boto3
import pandas as pd
import tempfile
from typing import Dict, List, Any

from helpers.aws_hack import hack_clear_aws_keys
from utils.failure_callbacks import slack_dag, sensor_timeout
from utils.gpg import init_gnupg
from utils.dictionaries import result_dict

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 17, 2),
    "retries": 0,
    "catchup": False,
    "on_failure_callback": slack_dag("slack_data_alerts"),
}

dag = DAG(
    dag_id="equifax_consumer_inbox",
    schedule_interval="@daily",
    default_args=default_args,
)

snowflake_conn = "airflow_production"

aws_hook = AwsBaseHook(aws_conn_id="s3_dataops", client_type="s3")
aws_credentials = aws_hook.get_credentials()

# Use first day of current month to determine last month name
today = datetime.now().today()
first = today.replace(day=1)
last_month = first - timedelta(days=1)

t_stamp = last_month.strftime("%Y%m")  # '2021XX'
base_file_name = f"tc_consumer_batch_{t_stamp}"
# bucket = "tc-datalake"
bucket = "tc-data-airflow-production"
full_response_path = "equifax_automated_batch/response/consumer"
full_output_path = "equifax_automated_batch/output/consumer"

table_name_raw = "equifax.output.consumer_batch_raw"
table_name_raw_history = f"equifax.output_history.consumer_batch_raw_{t_stamp}"
table_name = "equifax.output.consumer_batch"
table_name_history = f"equifax.output_history.consumer_batch_{t_stamp}"
table_name_public = "equifax.public.consumer_batch"

personal_info = [37, 52, 62, 88, 94, 124, 144, 146, 152]


def _check_if_file_downloaded() -> bool:
    return (
        False
        if Variable.get("equifax_consumer_response_downloaded") == "True"
        else True
    )


def _get_filename_from_remote() -> str:
    hook = SFTPHook(ftp_conn_id="equifax_sftp")
    # can safely assume only 1 file will be available every month as Equifax clears the directory after 7 days
    filename = hook.list_directory(path="outbox/")[0]
    filename_list = filename.split(".")
    filename_list.pop()
    filename_root = ".".join(filename_list)
    logging.info(filename_root)
    return filename_root


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


def upload_file_s3(file: Any, path: str) -> None:
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


def _generate_index_list(start: int, dol: Dict) -> List:
    result = [start]
    for l in dol:
        result.append(result[-1] + dol[l])
    return result[:-1]


def _convert_line_csv(line: str) -> str:
    indices = _generate_index_list(0, result_dict)
    parts = []
    x = zip(indices, indices[1:] + [None])
    for i, j in x:
        if j in personal_info:  # remove once we confirm that we do not need to mask personal info
            parts.append("")
        else:
            parts.append(line[i:j].strip().replace(",", "\,"))
    return ",".join(parts)


def convert_file() -> None:
    """
    Convert out1 file to csv according to our field dictionary
    """
    client = get_s3_client()
    # key = f"{full_response_path}/{base_file_name}.out1"
    # key = "equifax/consumer/decrypted/tc_consumer_batch_202107.out1"
    key = "equifax/consumer/decrypted/exthinkingpd.eqxcan.ds.scrdas.D20210817.T1620.txt"
    try:
        logging.info(f"Getting object {key} from {bucket}")
        file = client.get_object(
            Bucket=bucket,
            Key=key,
        )
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
                    lines.append(_convert_line_csv(line))
                    formatted.write(_convert_line_csv(line))
                    formatted.write("\n")

            upload_file_s3(
                formatted,
                # f"{full_output_path}/{base_file_name}.csv",
                # "equifax/consumer/csv/tc_consumer_batch_202107.csv",
                "equifax/consumer/csv/exthinkingpd.eqxcan.ds.scrdas.D20210817.T1620.csv",
            )
    except Exception as e:
        raise Exception(
            f"Unable to get object {key} from {bucket}: {e} or convert to csv"
        )


def _get_col_def(n: str, l: int, date_formatted: bool) -> str:
    if date_formatted and n in (
        "DOB_TEXT",
        "PRXX014",
        "PRXX016",
        "PRXX039",
        "PRXX044",
        "INQAL009",
        "INQAM009",
        "INQMG009",
        "INQBK009",
        "INQCU009",
        "INQNC009",
        "INQAF009",
        "INQPF009",
        "INQSF009",
        "INQRT009",
        "INQRD009",
        "INQTE009",
        "INQBD009",
        "INQCL009",
    ):
        return f"{n} date"
    return f"{n} varchar({l})"


def insert_snowflake(table: Any, file_name: str, date_formatted: bool = False) -> None:
    d3: Dict[str, int] = result_dict
    logging.info(f"Size of dict: {len(d3)}")

    cols = []
    value_cols = []
    for col, l in d3.items():
        cols.append(_get_col_def(col, l, date_formatted))
        value_cols.append(col)

    with SnowflakeHook("airflow_production").get_sqlalchemy_engine().begin() as sfh:
        if date_formatted:
            pass
            if "HISTORY" in table:
                sfh.execute(f"create or replace table {table} clone {table_name}")

            sql = f"create or replace table {table} ({','.join(cols)});"
            sfh.execute(sql)
        else:
            sql = f"create or replace table {table} ({','.join(cols)});"
            sfh.execute(sql)

        file = f"S3://{bucket}/{full_output_path}/{file_name}"
        copy = f"""
                COPY INTO {table} FROM {file} 
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
        sfh.execute(copy)


def insert_snowflake_raw() -> None:
    insert_snowflake(table_name_raw, f"{base_file_name}.csv")
    insert_snowflake(table_name_raw_history, f"{base_file_name}.csv")


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


def fix_date_format() -> None:
    """
    Date format of listed field are SAS format,
    and they are not valid to be converted into datetime directly with snowflake to_date()
    Therefore, we fix the format string value to make them convertable
    """
    with SnowflakeHook("airflow_production").get_sqlalchemy_engine().begin() as sfh:
        select = f"select * from {table_name_raw}"  # nosec
        result = sfh.execute(select)

        df = pd.DataFrame(result.cursor.fetchall())
        df.columns = [des[0] for des in result.cursor.description]
        for key in [
            "DOB_TEXT",
            "PRXX014",
            "PRXX016",
            "PRXX039",
            "PRXX044",
            "INQAL009",
            "INQAM009",
            "INQMG009",
            "INQBK009",
            "INQCU009",
            "INQNC009",
            "INQAF009",
            "INQPF009",
            "INQSF009",
            "INQRT009",
            "INQRD009",
            "INQTE009",
            "INQBD009",
            "INQCL009",
        ]:
            df[key] = df[key].apply(lambda x: _convert_date_format(x))
        with tempfile.NamedTemporaryFile(mode="w") as file_in:
            df.to_csv(file_in.name, index=False, sep=",")
            with open(file_in.name, "rb") as file:
                upload_file_s3(
                    file,
                    f"{full_output_path}/{base_file_name}.csv",
                )


def insert_snowflake_stage() -> None:
    insert_snowflake(table_name, f"{base_file_name}.csv", True)
    insert_snowflake(table_name_history, f"{base_file_name}.csv", True)


def insert_snowflake_public() -> None:
    columns = [
        "import_month",
        "accountid",
        "contractid",
        "business_name",
        *result_dict.keys(),
    ]
    columns_string = ",".join(columns)

    sql = f"""
            insert into {table_name_public}({columns_string})
            select '{t_stamp}' as import_month,
                    null as accountid,
                    null as contractid,
                    null as business_name,
                    u.*
            from {table_name} u
            """
    with SnowflakeHook("airflow_production").get_sqlalchemy_engine().begin() as sfh:
        sfh.execute(sql)


# short circuit if response file has already been downloaded for the month (file available for 7 days on server)
# list all directories on the remote system + pass filename via Xcoms
# check if response file is available for download on the sftp server (unnecessary?)
# download response file from sftp server to inbox/ folder
# check if the response file is available in the inbox/ folder (unnecessary?)
# decrypt the response file and upload to decrypted/ folder
# check if the decrypted response file is available

task_check_if_file_downloaded = ShortCircuitOperator(
    task_id="check_if_file_downloaded",
    python_callable=_check_if_file_downloaded,
    dag=dag,
)

task_get_filename_from_remote = PythonOperator(
    task_id="get_filename_from_remote",
    python_callable=_get_filename_from_remote,
    dag=dag,
)

task_is_response_file_available = SFTPSensor(
    task_id="is_response_file_available",
    path="outbox/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}.pgp",
    sftp_conn_id="equifax_sftp",
    poke_interval=5,
    timeout=5,
    dag=dag,
)

task_create_sftp_to_s3_job = SFTPToS3Operator(
    task_id="create_sftp_to_s3_job",
    sftp_conn_id="equifax_sftp",
    sftp_path="outbox/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}.pgp",
    s3_conn_id="s3_dataops",
    s3_bucket="tc-data-airflow-production",
    s3_key="equifax/consumer/inbox/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}.pgp",
    on_success_callback=_mark_response_as_downloaded,
    dag=dag,
)

task_decrypt_response_file = PythonOperator(
    task_id="decrypt_response_file",
    python_callable=_decrypt_response_file,
    op_kwargs={
        "s3_conn": "s3_dataops",
        "bucket_name": "tc-data-airflow-production",
        "download_key": "equifax/consumer/inbox/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}.pgp",
        "upload_key": "equifax/consumer/decrypted/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}",
    },
    dag=dag,
)

task_is_decrypted_response_file_available = S3KeySensor(
    task_id="is_decrypted_response_file_available",
    bucket_key="s3://tc-data-airflow-production/equifax/consumer/decrypted/{{ ti.xcom_pull(task_ids='get_filename_from_remote') }}.test",
    aws_conn_id="s3_dataops",
    poke_interval=5,
    timeout=20,
    on_failure_callback=sensor_timeout,
    dag=dag,
)

task_convert_file = PythonOperator(
    task_id="convert_file",
    python_callable=convert_file,
    dag=dag,
)

task_insert_snowflake_raw = PythonOperator(
    task_id="insert_snowflake_raw",
    python_callable=insert_snowflake_raw,
    dag=dag,
)

task_fix_date = PythonOperator(
    task_id="fix_date_format",
    python_callable=fix_date_format,
    dag=dag,
)

task_insert_snowflake_stage = PythonOperator(
    task_id="insert_snowflake_stage",
    python_callable=insert_snowflake_stage,
    dag=dag,
)

task_insert_snowflake_public = PythonOperator(
    task_id="insert_snowflake_public",
    python_callable=insert_snowflake_public,
    dag=dag,
)

(
    task_check_if_file_downloaded
    >> task_get_filename_from_remote
    >> task_is_response_file_available
    >> task_create_sftp_to_s3_job
    >> task_decrypt_response_file
    >> task_is_decrypted_response_file_available
)

(
    task_convert_file  # out1 -> csv -> s3
    >> task_insert_snowflake_raw  # copy from s3 to snowflake, table
    >> task_fix_date  # query table into dataframe, apply the format conversion, df-> csv, overwrite
    >> task_insert_snowflake_stage  # csv -> snowflake's equifax.output
    >> task_insert_snowflake_public  # equifax.output-> equifax.public
)
