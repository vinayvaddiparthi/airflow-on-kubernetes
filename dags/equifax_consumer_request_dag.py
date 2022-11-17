"""
#### Description
This dag generates request file for monthly Equifax consumer request file(.txt)
encoded in [windows-1252] or [iso-8859-1]
"""
from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

import logging
import tempfile
import pendulum
from datetime import timedelta
from pathlib import Path
from fs import open_fs, copy
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from jinja2 import Template

from equifax_extras.data import models
from equifax_extras.consumer import validation
from equifax_extras.consumer.request_file import RequestFile
from utils.failure_callbacks import slack_task


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(
        2020, 1, 1, tz=pendulum.timezone("America/Toronto")
    ),
    "concurrency": 1,
    "retries": 3,
    "on_failure_callback": slack_task("slack_data_alerts"),
}

dag = DAG(
    dag_id="equifax_consumer_request",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # Run once a month at midnight of the first day of the month
)
dag.doc_md = __doc__

snowflake_connection = "snowflake_production"
s3_connection = "s3_dataops"
output_bucket = "tc-data-airflow-production"
output_folder = "equifax/consumer/request"
validated_folder = "equifax/consumer/request_validated"

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
    applicant_file_number as (
      select
        fields:individuals_applicant_id::integer as applicant_id,
        fields:encrypted_value::string as encrypted_value
      from "ZETATANGO"."KYC_PRODUCTION"."INDIVIDUAL_ATTRIBUTES"
      where
        fields:key::string = 'file_number'
    ),
    applicant_with_attributes as (
      select
        applicant.applicant_guid,
        applicant_suffix.encrypted_value as encrypted_suffix,
        applicant_sin.encrypted_value as encrypted_sin,
        applicant_file_number.encrypted_value as encrypted_file_number
      from applicant
      left join applicant_sin on
        applicant.applicant_id = applicant_sin.applicant_id
      left join applicant_suffix on
        applicant.applicant_id = applicant_suffix.applicant_id
      left join applicant_file_number on
        applicant.applicant_id = applicant_file_number.applicant_id
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
        fields:id::integer                      as address_id,
        fields:city::string                     as city,
        fields:country_alpha_3::string          as country_alpha_3,
        fields:post_box_number::string          as post_box_number,
        fields:post_box_type::string            as post_box_type,
        fields:postal_code::string              as postal_code,
        fields:premise_number::string           as premise_number,
        fields:state_province::string           as state_province,
        fields:sub_premise_number::string       as sub_premise_number,
        fields:sub_premise_type::string         as sub_premise_type,
        fields:thoroughfare::string             as thoroughfare,
        fields:rural_routes::string             as rural_routes,
        fields:rural_additional_content::string as rural_additional_content
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
        encrypted_file_number,
        city,
        country_alpha_3,
        post_box_number,
        post_box_type,
        postal_code,
        premise_number,
        state_province,
        sub_premise_number,
        sub_premise_type,
        thoroughfare,
        rural_routes,
        rural_additional_content
      from eligible_applicant
      left join applicant_with_attributes on
        eligible_applicant.applicant_guid = applicant_with_attributes.applicant_guid
      left join applicant_with_address on
        eligible_applicant.applicant_guid = applicant_with_address.applicant_guid
      where eligible_applicant.applicant_guid not in ('app_JRagcMvXYyziw7bx', 'app_PcgRZsjdv3BvYTTU')  // remove applicants based on customer feedback
    )
select
    row_number() over (order by applicant_guid) as id,
    *
from final
"""


def _generate_file(
    snowflake_conn: str,
    s3_conn: str,
    bucket: str,
    folder: str,
    dag_run: DagRun,
    ds_nodash: str,
    **_: None,
) -> str:
    manual_process = ""
    if dag_run:
        config = dag_run.conf
        if config and "applicant_guids" in config:
            manual_list = config["applicant_guids"]
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

    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    query = session.query(models.Applicant, models.Address).from_statement(statement)
    results = query.all()

    local_dir = Path(tempfile.gettempdir()) / "equifax_batch" / "consumer"
    file_name = f"eqxds.exthinkingpd.ds.{ds_nodash}.txt"
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
    s3 = S3Hook(aws_conn_id=s3_conn)
    credentials = s3.get_credentials()
    dest_fs = open_fs(
        f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{folder}"
    )

    logging.info(f"Uploading {file_name} to {bucket}/{folder}.")

    copy.copy_file(src_fs, file_name, dest_fs, file_name)
    Variable.set("equifax_consumer_request_filename", file_name)
    Variable.set("equifax_consumer_request_sent", False)
    Variable.set("equifax_consumer_response_downloaded", False)
    return file_name


def _validate_file(
    s3_conn: str,
    bucket: str,
    folder: str,
    validated_folder: str,
    task_instance: TaskInstance,
    **_: None,
) -> None:
    """
    1. split file into header, footer, content lines
    2. check header: [len = 42, starts_with: BHDR-EQUIFAX, ends_with: ADVITFINSCOREDA2]
    3. check footer: [len = 48, starts_with: BTRL-EQUIFAX, ends_with: ADVITFINSCOREDA2, right padded: 8 digits of content lines count]
    4. check each content line: [len = 221]
    5. check content: [
        I: SIN: all numeric, or all empty space
        II: Date of Birth: all numeric, month between 1~12, day between 1~31
        III: Address: not empty
        IV: City/Province: all alphabet
        V: Postal code: alphanumeric, regex match [a-zA-Z]\\d[a-zA-Z]\\d[a-zA-Z]\\d
    ]
    If any of those above is wrong, tell the contact person in Risk let them decide if file is good to use
    """
    filename = task_instance.xcom_pull(task_ids="generate_file")
    s3 = S3Hook(aws_conn_id=s3_conn)
    credentials = s3.get_credentials()
    dest_fs = open_fs(
        f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{folder}"
    )
    with dest_fs.open(filename, mode="r", encoding="windows-1252") as file:
        error = validation.validate(file)

    keys = []
    for key in error:
        if error[key]:
            keys.append(key)
            logging.error(f"{key}: {error[key]}")
    if not len(keys):
        validated_fs = open_fs(
            f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{validated_folder}"
        )
        copy.copy_file(dest_fs, filename, validated_fs, filename)
        logging.info(
            f"Successfully uploaded request file to {bucket}/{validated_folder}"
        )
    else:
        raise AirflowFailException("Failed to validate request file")


generate_file = PythonOperator(
    task_id="generate_file",
    python_callable=_generate_file,
    op_kwargs={
        "snowflake_conn": snowflake_connection,
        "s3_conn": s3_connection,
        "bucket": output_bucket,
        "folder": output_folder,
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
    dag=dag,
)

validate_file = PythonOperator(
    task_id="validate_file",
    python_callable=_validate_file,
    op_kwargs={
        "s3_conn": s3_connection,
        "bucket": output_bucket,
        "folder": output_folder,
        "validated_folder": validated_folder,
    },
    execution_timeout=timedelta(hours=3),
    provide_context=True,
    dag=dag,
)

generate_file >> validate_file
