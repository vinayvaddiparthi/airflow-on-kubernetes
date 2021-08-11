# This dag generate request file to be sent to Equifax
# Scheduled at mid-night UTC of each month, (only send on odd month)
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

import logging
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from fs import open_fs, copy
from typing import Any
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from equifax_extras.data import models
from equifax_extras.commercial.request_file import RequestFile
from utils.failure_callbacks import slack_dag


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1, 00, 00, 00),
    "concurrency": 1,
    "retries": 3,
}

dag = DAG(
    dag_id="equifax_commercial_request",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # Run once a month at midnight of the first day of the month
    on_failure_callback=slack_dag("slack_data_alerts"),
)

snowflake_connection = "airflow_production"
s3_connection = "s3_dataops"
output_bucket = "tc-data-airflow-production"
output_folder = "equifax/commercial/request"

statement = text(
    """
with
    merchant as (
      select
        fields:name::string as name,
        fields:id::string as merchant_id,
        fields:entities_business_id::string as business_id,
        fields:guid::string as merchant_guid
      from "ZETATANGO"."KYC_PRODUCTION"."ENTITIES_MERCHANTS"
    ),
    business_file_number as (
      select
        fields:entities_businesses_id::string as business_id,
        fields:encrypted_value::string as encrypted_value
      from "ZETATANGO"."KYC_PRODUCTION"."ENTITIES_BUSINESS_ATTRIBUTES"
      where
        fields:key::string = 'file_number'
    ),
    merchant_with_attributes as (
      select
        merchant.merchant_guid,
        business_file_number.encrypted_value as encrypted_file_number
      from merchant
      left join business_file_number on
        merchant.business_id = business_file_number.business_id
    ),
    eligible_loan as (
      select
        merchant_guid,
        outstanding_balance,
        to_date(fully_repaid_at) as repaid_date,
        datediff(day, repaid_date, current_date()) as days_since_repaid
      from "ANALYTICS_PRODUCTION"."DBT_ARIO"."DIM_LOAN"
      where
        facility_code != 'O' and // Do not include Oppen loans
        ((days_since_repaid <= 365 and state = 'closed') or outstanding_balance <> 0.0)
    ),
    eligible_merchant as (
      select distinct
        merchant.name,
        merchant.merchant_id,
        merchant.merchant_guid
      from merchant
      inner join eligible_loan on
        merchant.merchant_guid = eligible_loan.merchant_guid
    ),
    address_relationship as (
      select
        fields:address_id::integer as address_id,
        fields:party_id::integer as merchant_id
      from "ZETATANGO"."KYC_PRODUCTION"."ADDRESS_RELATIONSHIPS"
      where
        fields:party_type::string = 'Entities::Merchant' and
        fields:active::string = 't' and
        fields:category::string = 'legal_business_address'
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
    merchant_with_address as (
      select
        eligible_merchant.merchant_guid,
        address.*
      from eligible_merchant
      left join address_relationship on
        eligible_merchant.merchant_id = address_relationship.merchant_id
      left join address on
        address_relationship.address_id = address.address_id
    ),
    phone_number_relationship as (
      select
        fields:phone_number_id::integer as phone_number_id,
        fields:party_id::integer as merchant_id
      from "ZETATANGO"."KYC_PRODUCTION"."PHONE_NUMBER_RELATIONSHIPS"
      where
        fields:party_type::string = 'Entities::Merchant' and
        fields:active::string = 't' and
        fields:category::string = 'business_phone_number'
    ),
    phone_number as (
      select
        fields:id::integer as phone_number_id,
        fields:area_code::string as area_code,
        fields:country_code::string as country_code,
        fields:phone_number::string as phone_number
      from "ZETATANGO"."KYC_PRODUCTION"."PHONE_NUMBERS"
    ),
    merchant_with_phone_number as (
      select
        eligible_merchant.merchant_guid,
        phone_number.*
      from eligible_merchant
      left join phone_number_relationship on
        eligible_merchant.merchant_id = phone_number_relationship.merchant_id
      left join phone_number on
        phone_number_relationship.phone_number_id = phone_number.phone_number_id
    ), 
    final as (
      select distinct
        eligible_merchant.merchant_guid,
        name,
        area_code,
        country_code,
        phone_number,
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
        encrypted_file_number
      from eligible_merchant
      left join merchant_with_address on
        eligible_merchant.merchant_guid = merchant_with_address.merchant_guid
      left join merchant_with_phone_number on
        eligible_merchant.merchant_guid = merchant_with_phone_number.merchant_guid
      left join merchant_with_attributes on
        eligible_merchant.merchant_guid = merchant_with_attributes.merchant_guid
    )
select
    row_number() over (order by merchant_guid) as id,
    *
from final
"""
)


def generate_file(
    snowflake_conn: str,
    s3_conn: str,
    bucket: str,
    folder: str,
    ds_nodash: str,
    **_: Any,
) -> None:
    """
    Snowflake -> TempDir -> S3 bucket
    As of June 22, we still need to copy the generated request file to another folder in the S3 bucket:
    tc-data-airflow-production/equifax/commercial/outbox.
    """
    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    query = session.query(
        models.Merchant, models.Address, models.PhoneNumber
    ).from_statement(statement)
    results = query.all()

    local_dir = Path(tempfile.gettempdir()) / "equifax_batch" / "commercial"
    file_name = f"eqxcom.exthinkingpd.TCAP.{ds_nodash}.csv"
    request_file = RequestFile(local_dir / file_name)

    request_file.write_header()
    merchant_guids = set([result.Merchant.guid for result in results])
    logging.info(
        f"Generating {len(results)} lines for {len(merchant_guids)} merchants..."
    )
    for result in results:
        merchant = result.Merchant
        address = result.Address
        phone_number = result.PhoneNumber
        request_file.append(merchant, address, phone_number)
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


task_generate_file = PythonOperator(
    task_id="generate_file",
    python_callable=generate_file,
    op_kwargs={
        "snowflake_conn": snowflake_connection,
        "s3_conn": s3_connection,
        "bucket": output_bucket,
        "folder": output_folder,
    },
    executor_config={
        "resources": {
            "requests": {"memory": "512Mi"},
            "limits": {"memory": "1Gi"},
        },
    },
    execution_timeout=timedelta(hours=3),
    provide_context=True,
    dag=dag,
)
