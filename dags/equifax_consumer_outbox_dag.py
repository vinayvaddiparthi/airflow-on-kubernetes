"""
# Equifax Consumer Outbox DAG

This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator

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
    'start_date': datetime(2021, 1, 1),
    'catchup': False,
    'tags': ['equifax'],
    'description': "A workflow to send the consumer batch request file to Equifax",
}

dag = DAG(
    dag_id='equifax_consumer_outbox',
    default_args=default_args,
    schedule_interval="@daily",
)
dag.doc_md = __doc__

s3_connection = 's3_dataops'
sftp_connection = 'equifax_sftp'
S3_BUCKET = 'tc-data-airflow-production'
S3_KEY = '/consumer/outbox/eqxds.exthinkingpd.ds.20210801.txt'

# task 1 - check if s3 folder (/outbox) contains request file for this month

# task 2a - if the request file for this month exists, then send the file to Equifax
task_create_s3_to_sftp_job = S3ToSFTPOperator(
    task_id='create_s3_to_sftp_job',
    sftp_conn_id=sftp_connection,
    sftp_path='inbox/',
    s3_conn_id=s3_connection,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
)
# task 2b - if the request file for this month does not exist, then proceed to Dummy Operator