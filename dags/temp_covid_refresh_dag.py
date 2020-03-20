import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from import_sfdc import ctas_to_snowflake, create_sf_summary_table

from salesforce_import_extras.sobjects.sfoi import sobjects

instance = "sfoi"

with DAG(
    "temp_covid_refresh_dag",
    start_date=pendulum.datetime(
        2019, 3, 20, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval=None,
    catchup=False,
) as dag:
    for sobject in [
        x
        for x in sobjects
        if x["name"] in ["account", "opportunity", "collections_issue__c", "pricing__c"]
    ]:
        dag << PythonOperator(
            task_id=f'snowflake__{sobject["name"]}',
            python_callable=ctas_to_snowflake,
            op_kwargs={"sfdc_instance": instance, "sobject": sobject},
            pool=f"{instance}_pool",
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
