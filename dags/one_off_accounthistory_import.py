from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
from sqlalchemy import create_engine

query = """
insert into sf_salesforce.sfoi_raw.accounthistory select
cast(id as varchar(18)) as id,
FALSE as isdeleted,
cast(accountid as varchar(18)),
cast(created_by_id as varchar(18)) as createdbyid,
created_date as createddate,
cast(field as varchar(255)),
cast(old_value as varchar(16777216)) as oldvalue,
cast(new_value as varchar(16777216)) as newvalue
from "redshift"."sf_staging_new"."account_history"
"""

def runq():
    engine = create_engine("presto://presto-production-internal.presto.svc:8080")

    with engine.begin() as tx:
        tx.execute(query).fetchall()

def create_dag():
    with DAG(
        f"accounthistory_import",
        start_date=pendulum.datetime(
            2020, 1, 6, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=None,
        catchup=False,
    ) as dag:
        dag << PythonOperator(
            task_id=f"runq",
            python_callable=runq),
        # on_failure_callback=slack_on_fail,

    return dag

globals()["accounthistory_import"] = create_dag()
