import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from salesforce_import_extras.sobjects import sobjects
from salesforce_import_extras.common_functions import ctas_to_glue, ctas_to_snowflake, \
    create_sf_summary_table

instance = "sfoi"

with DAG(
    f"tli_refresh",
    start_date=pendulum.datetime(
        2019, 10, 11, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="20,50 * * * *",
) as dag:
    for t in ["c2g__codatransactionlineitem__c"]:
        dag << PythonOperator(
            task_id=f"glue__{t}",
            python_callable=ctas_to_glue,
            op_kwargs={"sfdc_instance": instance, "sobject": t},
            pool=f"{instance}_pool",
        ) >> PythonOperator(
            task_id=f"snowflake__{t}",
            python_callable=ctas_to_snowflake,
            op_kwargs={"sfdc_instance": instance, "sobject": t},
            pool="snowflake_pool",
        ) >> PythonOperator(
            task_id=f"snowflake_summary__{t}",
            python_callable=create_sf_summary_table,
            op_kwargs={
                "conn": "snowflake_default",
                "sfdc_instance": instance,
                "sobject": t,
            },
            pool="snowflake_pool",
        )
