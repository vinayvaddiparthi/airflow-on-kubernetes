"""
# Equifax Consumer Outbound DAG

This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""
from airflow import DAG

from datetime import datetime, timedelta

from utils.failure_callbacks import slack_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['enterprisedata@thinkingcapital.ca'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_dag('slack_data_alerts'),
}

dag = DAG(
    dag_id='equifax_consumer_outbound',
    default_args=default_args,
    description="A workflow to send the consumer batch request file to Equifax",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 15),
    catchup=False,
    tags=['equifax'],
)
dag.doc_md = __doc__

# task 1 - check if s3 folder (/outbox) contains request file for this month

# task 2a - if the request file for this month exists, then send the file to Equifax

# task 2b - if the request file for this month does not exist, then proceed to Dummy Operator