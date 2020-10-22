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
from equifax_extras.commercial import RequestFile

from typing import Any


statement = text(
    """
with
    merchant as (
      select
        fields:name::string as name,
        fields:id::string as merchant_id,
        fields:guid::string as merchant_guid
      from "ZETATANGO"."KYC_PRODUCTION"."ENTITIES_MERCHANTS"
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
        merchant.merchant_guid,
        address.*
      from merchant
      left join address_relationship on
        merchant.merchant_id = address_relationship.merchant_id
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
        merchant.merchant_guid,
        phone_number.*
      from merchant
      left join phone_number_relationship on
        merchant.merchant_id = phone_number_relationship.merchant_id
      left join phone_number on
        phone_number_relationship.phone_number_id = phone_number.phone_number_id
    ),
    
    loan as (
      select
        merchant_guid
      from "ANALYTICS_PRODUCTION"."DBT_ARIO"."DIM_LOAN"
      where
        state='repaying'
    ),
    repaying_merchant as (
      select distinct
        merchant.name,
        merchant.merchant_guid
      from merchant inner join loan on
        merchant.merchant_guid = loan.merchant_guid
    ),
    
    final as (
      select distinct
        repaying_merchant.merchant_guid,
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
        thoroughfare
      from repaying_merchant
      left join merchant_with_address on
        repaying_merchant.merchant_guid = merchant_with_address.merchant_guid
      left join merchant_with_phone_number on
        repaying_merchant.merchant_guid = merchant_with_phone_number.merchant_guid
    )
select
    row_number() over (order by merchant_guid) as id,
    *
from final
"""
)


def generate_file(
    snowflake_connection: str,
    s3_connection: str,
    bucket: str,
    folder: str,
    **context: Any,
) -> None:
    engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    query = session.query(
        models.Merchant, models.Address, models.PhoneNumber
    ).from_statement(statement)
    results = query.all()

    local_dir = Path(tempfile.gettempdir()) / "equifax_batch"
    local_dir.mkdir(exist_ok=True)
    file_name = f"equifax_batch_commercial_request_{context['dag_run'].run_id}.csv"
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
    s3 = S3Hook(aws_conn_id=s3_connection)
    credentials = s3.get_credentials()
    dest_fs = open_fs(
        f"s3://{credentials.access_key}:{credentials.secret_key}@{bucket}/{folder}"
    )

    logging.info(f"Uploading {file_name} to {bucket}/{folder}.")
    copy.copy_file(src_fs, file_name, dest_fs, file_name)


def create_dag() -> DAG:
    default_args = {
        "owner": "airflow",
        "start_date": datetime(2020, 1, 1, 00, 00, 00),
        "concurrency": 1,
        "retries": 3,
    }

    with DAG(
        dag_id="equifax_batch_commercial_request",
        catchup=False,
        default_args=default_args,
        schedule_interval="0 0 1 * *",  # Run once a month at midnight of the first day of the month
    ) as dag:
        op_generate_file = PythonOperator(
            task_id="generate_file",
            python_callable=generate_file,
            op_kwargs={
                "snowflake_connection": "snowflake_analytics_production",
                "s3_connection": "s3_datalake",
                "bucket": "tc-datalake",
                "folder": "equifax_automated_batch/request/commercial",
            },
            execution_timeout=timedelta(hours=3),
            provide_context=True,
        )

        dag << op_generate_file

        return dag


environment = Variable.get("environment", "")
if environment == "development":
    from equifax_extras.utils.local_get_sqlalchemy_engine import (
        local_get_sqlalchemy_engine,
    )

    SnowflakeHook.get_sqlalchemy_engine = local_get_sqlalchemy_engine

if __name__ == "__main__":
    from collections import namedtuple

    MockDagRun = namedtuple("MockDagRun", ["run_id"])
    timestamp = datetime.now()
    time_tag = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    mock_context = {"dag_run": MockDagRun(time_tag)}

    generate_file(
        snowflake_connection="airflow_production",
        s3_connection="s3_datalake",
        bucket="tc-datalake",
        folder="equifax_automated_batch/request/commercial",
        **mock_context,
    )
else:
    globals()["equifax_batch_commercial_request"] = create_dag()
