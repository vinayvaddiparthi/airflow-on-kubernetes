import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from salesforce_import_extras.common_functions import (
    ctas_to_glue,
    ctas_to_snowflake,
    create_sf_summary_table,
)

instance = "sfoi"

with DAG(
    f"tli_refresh",
    start_date=pendulum.datetime(
        2019, 10, 11, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="10,50 8-18 * * 1-5",
    catchup=False,
) as dag:
    for sobject in [{"name": "c2g__codatransactionlineitem__c"}]:
        dag << PythonOperator(
            task_id=f'glue__{sobject["name"]}',
            python_callable=ctas_to_glue,
            op_kwargs={"sfdc_instance": instance, "sobject": sobject},
            pool=f"{instance}_pool",
        ) >> PythonOperator(
            task_id=f'snowflake__{sobject["name"]}',
            python_callable=ctas_to_snowflake,
            op_kwargs={"sfdc_instance": instance, "sobject": sobject},
            pool="snowflake_pool",
        ) >> PythonOperator(
            task_id=f'snowflake_summary__{sobject["name"]}',
            python_callable=create_sf_summary_table,
            op_kwargs={
                "conn": "snowflake_default",
                "sfdc_instance": instance,
                "sobject": sobject,
            },
            pool="snowflake_pool",
        )
