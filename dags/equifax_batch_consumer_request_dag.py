import logging
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from fs import open_fs, copy

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from equifax_extras.data import models
from equifax_extras.consumer import validation
from equifax_extras.consumer.request_file import RequestFile

from typing import Any
from jinja2 import Template

statement_template = """
with
    applicant as (
      select
        fields:id::integer as applicant_id,
        fields:guid::string as applicant_guid, 
        fields:encrypted_date_of_birth::string as encrypted_date_of_birth,
        fields:encrypted_first_name::string as encrypted_first_name,
        fields:encrypted_last_name::string as encrypted_last_name,
        fields:encrypted_middle_name::string as encrypted_middle_name
      from "ZETATANGO"."KYC_PRODUCTION"."INDIVIDUALS_APPLICANTS"
    ),
    applicant_sin as (
      select
        fields:individuals_applicant_id::integer as applicant_id,
        fields:encrypted_value::string as encrypted_value
      from "ZETATANGO"."KYC_PRODUCTION"."INDIVIDUAL_ATTRIBUTES"
      where
        fields:key::string = 'sin'
    ),
    applicant_suffix as (
      select
        fields:individuals_applicant_id::integer as applicant_id,
        fields:encrypted_value::string as encrypted_value
      from "ZETATANGO"."KYC_PRODUCTION"."INDIVIDUAL_ATTRIBUTES"
      where
        fields:key::string = 'suffix'
    ),
    applicant_with_attributes as (
      select
        applicant.applicant_guid,
        applicant_suffix.encrypted_value as encrypted_suffix,
        applicant_sin.encrypted_value as encrypted_sin
      from applicant
      left join applicant_sin on
        applicant.applicant_id = applicant_sin.applicant_id
      left join applicant_suffix on
        applicant.applicant_id = applicant_suffix.applicant_id
    ),
    address_relationship as (
      select
        fields:address_id::integer as address_id,
        fields:party_id::integer as applicant_id
      from "ZETATANGO"."KYC_PRODUCTION"."ADDRESS_RELATIONSHIPS"
      where
        fields:party_type::string = 'Individuals::Applicant' and
        fields:active::string = 't' and
        fields:category::string = 'physical_address'
    ),
    address as (
      select
        fields:id::integer as address_id,
        fields:city::string as city,
        fields:country_alpha_3::string as country_alpha_3,
        fields:post_box_number::string as post_box_number,
        fields:post_box_type::string as post_box_type,
        fields:postal_code::string as postal_code,
        fields:premise_number::string as premise_number,
        fields:state_province::string as state_province,
        fields:sub_premise_number::string as sub_premise_number,
        fields:sub_premise_type::string as sub_premise_type,
        fields:thoroughfare::string as thoroughfare
      from "ZETATANGO"."KYC_PRODUCTION"."ADDRESSES"
    ),
    applicant_with_address as (
      select
        applicant.applicant_guid,
        address.*
      from applicant
      left join address_relationship on
        applicant.applicant_id = address_relationship.applicant_id
      left join address on
        address_relationship.address_id = address.address_id
    ),
    eligible_loan as (
      select
        merchant_guid,
        outstanding_balance,
        to_date(fully_repaid_at) as repaid_date,
        datediff(day, repaid_date, current_date()) as days_since_repaid
      from "ANALYTICS_PRODUCTION"."DBT_ARIO"."DIM_LOAN"
      where
        // Do not include Oppen loans
        facility_code != 'O' and
        ((days_since_repaid <= 365 and state = 'closed') or outstanding_balance <> 0.0)
    ),
    eligible_merchant as (
      select distinct
        merchant.name,
        merchant.guid as merchant_guid,
        merchant.primary_applicant_guid
      from "ANALYTICS_PRODUCTION"."DBT_ARIO"."DIM_MERCHANT" as merchant
      inner join eligible_loan on
        merchant.guid = eligible_loan.merchant_guid
    ),
    eligible_applicant as (
      select
        applicant.*
      from applicant
      inner join eligible_merchant on
        eligible_merchant.primary_applicant_guid = applicant.applicant_guid
      {{manual_process}}
    ),
    final as (
      select distinct
        eligible_applicant.applicant_guid,
        encrypted_date_of_birth,
        encrypted_first_name,
        encrypted_last_name,
        encrypted_middle_name,
        encrypted_suffix,
        encrypted_sin,
        city,
        country_alpha_3,
        post_box_number,
        post_box_type,
        postal_code,
        premise_number,
        state_province,
        sub_premise_number,
        sub_premise_type,
        thoroughfare
      from eligible_applicant
      left join applicant_with_attributes on
        eligible_applicant.applicant_guid = applicant_with_attributes.applicant_guid
      left join applicant_with_address on
        eligible_applicant.applicant_guid = applicant_with_address.applicant_guid
    )
select
    row_number() over (order by applicant_guid) as id,
    *
from final
"""


def generate_file(
    snowflake_connection: str,
    s3_connection: str,
    bucket: str,
    folder: str,
    **context: Any,
) -> str:
    manual_process = ""
    conf = context["dag_run"].conf
    if conf and "applicant_guids" in conf:
        manual_list = conf["applicant_guids"]
        if manual_list:
            sub_query = """
            union
            select applicant.* 
            from applicant where applicant.applicant_guid in {{applicant_guids}}
            """
            manual_process = Template(sub_query).render(
                applicant_guids=tuple(manual_list)
            )
    statement = text(Template(statement_template).render(manual_process=manual_process))
    engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    query = session.query(models.Applicant, models.Address).from_statement(statement)
    results = query.all()

    local_dir = Path(tempfile.gettempdir()) / "equifax_batch" / "consumer"
    file_name = f"equifax_batch_consumer_request_{context['dag_run'].run_id}.txt"
    request_file = RequestFile(local_dir / file_name)

    request_file.write_header()
    applicant_guids = set([result.Applicant.guid for result in results])
    logging.info(
        f"Generating {len(results)} lines for {len(applicant_guids)} applicants..."
    )
    for result in results:
        applicant = result.Applicant
        address = result.Address
        request_file.append(applicant, address)
    request_file.write_footer()
    logging.info(f"File {file_name} generated successfully.")

    # Upload request file to S3
    src_fs = open_fs(str(local_dir))
    s3 = S3Hook(aws_conn_id=s3_connection)
    credentials = s3.get_credentials()
    dest_fs = open_fs(
        f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{folder}"
    )

    logging.info(f"Uploading {file_name} to {bucket}/{folder}.")
    copy.copy_file(src_fs, file_name, dest_fs, file_name)
    return file_name


def validate_file(
    s3_connection: str,
    bucket: str,
    folder: str,
    **context: Any,
) -> None:
    """
    1. split file into header, footer, content lines
    2. check header: [len = 42, starts_with: BHDR-EQUIFAX, ends_with: ADVITFINSCOREDA2]
    3. check footer: [len = 48, starts_with: BTRL-EQUIFAX, ends_with: ADVITFINSCOREDA2, right padded: 8 digits of content lines count]
    4. check each content line: [len = 221]
    5. check content: [
        I: SIN: all numeric, or all empty space
        II: Date of Birth: all numeric, month between 1~12, day between 1~31
        III: City/Province: all alphabet
        IV: Postal code: alphanumeric, regex match [a-zA-Z]\\d[a-zA-Z]\\d[a-zA-Z]\\d
    ]
    """
    filename = context["task_instance"].xcom_pull(task_ids="pushing_task")
    s3 = S3Hook(aws_conn_id=s3_connection)
    credentials = s3.get_credentials()
    dest_fs = open_fs(
        f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{folder}"
    )
    with dest_fs.open(filename, mode="r", encoding="windows-1252") as file:
        validation.validate(file)


def create_dag(bucket: str, folder: str) -> DAG:
    default_args = {
        "owner": "airflow",
        "start_date": datetime(2020, 1, 1, 00, 00, 00),
        "concurrency": 1,
        "retries": 3,
    }

    with DAG(
        dag_id="equifax_batch_consumer_request",
        catchup=False,
        default_args=default_args,
        schedule_interval="0 0 1 * *",  # Run once a month at midnight of the first day of the month
    ) as dag:
        op_generate_file = PythonOperator(
            task_id="generate_file",
            python_callable=generate_file,
            op_kwargs={
                "snowflake_connection": "airflow_production",
                "s3_connection": "s3_datalake",
                "bucket": bucket,
                "folder": folder,
            },
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowProductionZetatangoPiiRole"
                    }
                },
                "resources": {
                    "requests": {"memory": "512Mi"},
                    "limits": {"memory": "1Gi"},
                },
            },
            execution_timeout=timedelta(hours=3),
            provide_context=True,
        )

        op_validate_file = PythonOperator(
            task_id="validate_file",
            python_callable=validate_file,
            op_kwargs={
                "s3_connection": "s3_datalake",
                "bucket": bucket,
                "folder": folder,
            },
            execution_timeout=timedelta(hours=3),
            provide_context=True,
        )
        dag << op_generate_file >> op_validate_file

        return dag


environment = Variable.get("environment", "")
if environment == "development":
    from equifax_extras.utils.local_get_sqlalchemy_engine import (
        local_get_sqlalchemy_engine,
    )

    SnowflakeHook.get_sqlalchemy_engine = local_get_sqlalchemy_engine
    output_bucket = "tc-datalake"
    output_folder = "equifax_automated_batch/request/consumer/test"
else:
    output_bucket = "tc-datalake"
    output_folder = "equifax_automated_batch/request/consumer"

if __name__ == "__main__":
    from collections import namedtuple

    MockDagRun = namedtuple("MockDagRun", ["run_id"])
    timestamp = datetime.now()
    time_tag = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    mock_context = {"dag_run": MockDagRun(time_tag)}

    generate_file(
        snowflake_connection="airflow_production",
        s3_connection="s3_datalake",
        bucket=output_bucket,
        folder=output_folder,
        **mock_context,
    )
else:
    globals()["equifax_batch_consumer_request"] = create_dag(
        output_bucket, output_folder
    )
