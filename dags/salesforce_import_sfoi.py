import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from salesforce_import_extras.sobjects import sobjects
from salesforce_import_extras.common_functions import ctas_to_glue, ctas_to_snowflake


sfdc_instance = "sfoi"

with DAG(
    f"{sfdc_instance}_to_glue_import",
    start_date=datetime.datetime(2019, 10, 9),
    schedule_interval=None,
) as dag:
    for t in sobjects[sfdc_instance]:
        dag << PythonOperator(
            task_id=f"glue__{t}",
            python_callable=ctas_to_glue,
            op_kwargs={"sfdc_instance": sfdc_instance, "sobject": t},
            pool=f"{sfdc_instance}_pool",
        ) >> PythonOperator(
            task_id=f"snowflake__{t}",
            python_callable=ctas_to_snowflake,
            op_kwargs={"sfdc_instance": sfdc_instance, "sobject": t},
            pool="snowflake_pool",
        )
