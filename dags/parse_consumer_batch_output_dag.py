from datetime import datetime, timedelta
import logging

from helpers.aws_hack import hack_clear_aws_keys

from equifax_extras.consumer.response_format import *

import boto3
import pandas as pd
import tempfile
from typing import Dict, List, Any
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable

today = datetime.now().today()
first = today.replace(day=1)
last_month = first - timedelta(days=1)


Variable.set("t_stamp", last_month.strftime("%Y%m"))
#timestamp = Variable.get('t_stamp')
timestamp = "202102"
base_file_name = f"tc_consumer_batch_{timestamp}"
bucket = "tc-datalake"
prefix_path = "equifax_automated_batch"
response_path = "/response"
output_path = "/output"
consumer_path = "/consumer"
full_response_path = prefix_path + response_path + consumer_path
full_output_path = prefix_path + output_path + consumer_path

table_name_raw = "EQUIFAX.OUTPUT.CONSUMER_BATCH_RAW"
table_name_raw_history = f"EQUIFAX.OUTPUT_HISTORY.CONSUMER_BATCH_RAW_{timestamp}"
table_name = "EQUIFAX.OUTPUT.CONSUMER_BATCH"
table_name_history = f"EQUIFAX.OUTPUT_HISTORY.CONSUMER_BATCH_{timestamp}"
table_name_public = "EQUIFAX.PUBLIC.CONSUMER_BATCH"

personal_info = [37, 52, 62, 88, 94, 124, 144, 146, 152]

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 17, 2),
    "retries": 0,
}

dag = DAG(
    "equifax_consumer_batch_output_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)

aws_hook = AwsHook(aws_conn_id="s3_datalake")
aws_credentials = aws_hook.get_credentials()


def _convert_line_csv(line: str) -> str:
    indices = _gen_arr(0, result_dict_1) + _gen_arr(94, result_dict_2)
    parts = []
    x = zip(indices, indices[1:] + [None])
    for i, j in x:
        if j in personal_info:
            parts.append("")
        else:
            parts.append(line[i:j].strip().replace(",", "\,"))
    return ",".join(parts)


def _gen_arr(start: int, dol: Dict) -> List:
    result = [start]
    for l in dol:
        result.append(result[-1] + dol[l])
    return result[:-1]


def _get_col_def(n: str, l: int) -> str:
    return f"{n} varchar({l})"


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


def _get_s3() -> Any:
    hack_clear_aws_keys()
    return boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )


def _get_snowflake() -> Any:
    snowflake_conn = "airflow_production"
    return SnowflakeHook(snowflake_conn).get_sqlalchemy_engine().begin()


def _insert_snowflake(table: Any, file_name: str, date_formatted: bool = False) -> None:
    d3: Dict[str, int] = result_dict
    logging.info(f"Size of dict: {len(d3)}")

    cols = []
    value_cols = []
    for col in d3:
        cols.append(_get_col_def(col, d3[col]))
        value_cols.append(col)

    with _get_snowflake() as sfh:
        if date_formatted:
            pass
            if "HISTORY" in table:
                sfh.execute(f"CREATE OR REPLACE TABLE {table} CLONE {table_name}")

            truncate = f"TRUNCATE TABLE {table}"
            sfh.execute(truncate)
        else:
            sql = f"CREATE OR REPLACE TABLE {table} ({','.join(cols)});"
            sfh.execute(sql)

        # insert = f"INSERT INTO {table} ({','.join(value_cols)}) VALUES{','.join(values)};"
        copy = f"""COPY INTO {table} FROM S3://{bucket}/{full_output_path}/{file_name} CREDENTIALS = (
                                            aws_key_id='{aws_credentials.access_key}',
                                            aws_secret_key='{aws_credentials.secret_key}')
                                            FILE_FORMAT=(field_delimiter=',', FIELD_OPTIONALLY_ENCLOSED_BY = '"'{', skip_header=1' if date_formatted else ''})
                                            """
        # result = sfh.execute(insert)
        sfh.execute(copy)


def fix_date_format() -> None:
    with _get_snowflake() as sfh:
        select = f"SELECT * FROM {table_name_raw}"  # nosec
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


def check_output(**kwargs: Dict) -> str:
    client = _get_s3()
    try:
        file = client.get_object(
            Bucket=bucket,
            Key=f"{full_output_path}/{base_file_name}.csv",
        )
        logging.info(file)
        return "end"
    except:
        return "get_input"


def get_input(**kwargs: Dict) -> str:
    client = _get_s3()
    key = f"{full_response_path}/{base_file_name}.out1"
    try:
        logging.info(f"Getting object {key} from {bucket}")
        file = client.get_object(
            Bucket=bucket,
            Key=key,
        )
        body = file["Body"].read()
        content = body.decode("ISO-8859-1")
        Variable.set("file_content", content)
        return "convert_file"
    except Exception as e:
        logging.warning(f"Unable to get object {key} from {bucket}: {e}")
        return "end"


def convert_file() -> None:
    with tempfile.TemporaryFile(
        mode="w+", encoding="ISO-8859-1"
    ) as raw, tempfile.NamedTemporaryFile(
        mode="w+", encoding="ISO-8859-1"
    ) as formatted:
        content = Variable.get("file_content")
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
            f"{full_output_path}/{base_file_name}.csv",
        )


def upload_file_s3(file: Any, path: str) -> None:
    file.seek(0)
    try:
        client = _get_s3()
        client.upload_file(
            file.name,
            bucket,
            path,
        )
    except:
        logging.error("Error when uploading file to s3")


def insert_snowflake_public() -> None:
    sql = f"""
            INSERT INTO {table_name_public}
            SELECT '{Variable.get('t_stamp')}' as import_month,
                    NULL as accountid,
                    NULL as contractid,
                    NULL as business_name,
                    u.*
            FROM {table_name} u
            """

    with _get_snowflake() as sfh:
        sfh.execute(sql)


def insert_snowflake_raw() -> None:
    _insert_snowflake(table_name_raw, f"{base_file_name}.csv")
    _insert_snowflake(table_name_raw_history, f"{base_file_name}.csv")


def insert_snowflake() -> None:
    _insert_snowflake(table_name, f"{base_file_name}.csv", True)
    _insert_snowflake(table_name_history, f"{base_file_name}.csv", True)


def end() -> None:
    pass


task_check_output = BranchPythonOperator(
    task_id="check_output",
    python_callable=check_output,
    provide_context=True,
    dag=dag,
)

task_get_file = BranchPythonOperator(
    task_id="get_input",
    python_callable=get_input,
    provide_context=True,
    dag=dag,
)

task_convert_file = PythonOperator(
    task_id="convert_file", python_callable=convert_file, dag=dag
)

task_fix_date = PythonOperator(
    task_id="fix_date_format",
    python_callable=fix_date_format,
    dag=dag,
)

task_insert_snowflake_raw = PythonOperator(
    task_id="insert_snowflake_raw", python_callable=insert_snowflake_raw, dag=dag
)

task_insert_snowflake = PythonOperator(
    task_id="insert_snowflake", python_callable=insert_snowflake, dag=dag
)

task_insert_snowflake_public = PythonOperator(
    task_id="insert_snowflake_public", python_callable=insert_snowflake_public, dag=dag
)

task_end = PythonOperator(task_id="end", python_callable=end, dag=dag)

task_check_output >> [task_get_file, task_end]
task_get_file >> [task_convert_file, task_end]
task_convert_file >> task_insert_snowflake_raw >> task_fix_date >> task_insert_snowflake >> task_insert_snowflake_public

environment = Variable.get("environment", "")
if environment == "development":
    from equifax_extras.utils.local_get_sqlalchemy_engine import (
        local_get_sqlalchemy_engine,
    )

    SnowflakeHook.get_sqlalchemy_engine = local_get_sqlalchemy_engine


def clone_table():
    t = "EQUIFAX.PUBLIC.CONSUMER_BATCH"
    t_new = "EQUIFAX.PUBLIC.BVTEST_CONSUMER_BATCH"
    sql = f"""
            CREATE TABLE IF NOT EXISTS {t_new} CLONE {t}
        """
    role = "DBT_DEVELOPMENT"

    with _get_snowflake() as sfh:
        sfh.execute(sql)
        sfh.execute(f"GRANT OWNERSHIP ON TABLE {t_new} TO ROLE {role}")


if __name__ == "__main__":
    # get_input()
    # convert_file()
    # insert_snowflake_raw()
    # fix_date_format()
    # insert_snowflake()
    # insert_snowflake_public()

    # clone_table()

    # get_input()
    # convert_file()
    # _insert_snowflake("EQUIFAX.OUTPUT.BVTEST_CONSUMER_BATCH", f"{base_file_name}.csv", True)

    table = "EQUIFAX.PUBLIC.BVTEST_CONSUMER_BATCH"
    sql = f"""
        ALTER TABLE {table} RENAME COLUMN FILLER2 TO APPLICANT_GUID
"""
    with _get_snowflake() as sfh:
        sfh.execute(sql)


    # insert_snowflake()
