import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from salesforce_import_extras.sobjects import sobjects
from salesforce_import_extras.common_functions import ctas_to_glue, ctas_to_snowflake


catalog = "sfoi"

with DAG(
    f"{catalog}_to_glue_import",
    start_date=datetime.datetime(2019, 10, 9),
    schedule_interval=None,
) as dag:
    for t in sobjects[catalog]:
        dag << PythonOperator(
            task_id=f"glue__{t}",
            python_callable=ctas_to_glue,
            op_kwargs={"catalog": catalog, "sobject": t},
            pool=f"{catalog}_pool",
        ) >> PythonOperator(
            task_id=f"snowflake__{t}",
            python_callable=ctas_to_snowflake,
            op_kwargs={"catalog": catalog, "sobject": t},
            pool="snowflake_pool",
        )
