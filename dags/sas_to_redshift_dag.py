from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from sas_to_redshift_dag_extras.operator_query import QueryOperator
from sas_to_redshift_dag_extras.s3_copy_operator import S3CopyOperator

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 3),
    "retries": 0,
}

dag = DAG("sas_to_redshift_dag", schedule_interval="@daily", default_args=default_args)

sobjects = [
    "account_owner",
    "account_status",
    "contract_history",
    "days_non_payment",
    "days_past_due",
    "loan_account",
    "loan_account_summary",
    "principal_balance",
    "total_balance",
]


def is_positive(n):
    if n >= 0:
        return None
    else:
        return "Rows count dropped, please investigate."


def check_drop(name):
    return QueryOperator(
        task_id=f"check_drop_{name}",
        sql=f"select (select count(*) as c from sas.{name}_bucket) -(select count(*) as c from sas.{name});",
        postgres_conn_id="redshift_tc_dw",
        callable=is_positive,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag,
    )


def swap_table(name):
    return PostgresOperator(
        task_id=f"swap_table_{name}",
        sql=[
            f"alter table sas.{name}_old rename to {name}_temp",
            f"alter table sas.{name} rename to {name}_old",
            f"alter table sas.{name}_bucket rename to {name}",
            f"truncate table sas.{name}_temp",
            f"alter table sas.{name}_temp rename to {name}_bucket",
        ],
        postgres_conn_id="redshift_tc_dw",
        database="tc_datawarehouse",
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag,
    )


def copy_table(name):
    return S3CopyOperator(
        task_id=f"copy_table_{name}",
        schema="tc_datawarehouse",
        table=f"sas.{name}_bucket",
        path=f"tc-datalake/sas/external_tables/{name}/{name}.txt",
        delimiter="|",
        postgres_conn_id="redshift_tc_dw",
        s3_conn_id="s3_sfdc_import_staging",
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag,
    )


task_tli_1 = S3CopyOperator(
    task_id="copy_tli_1",
    schema="tc_datawarehouse",
    table="sas.tli_categorized_bucket",
    path="tc-datalake/sas/external_tables/tli/tli_categorized1.txt",
    delimiter="|",
    postgres_conn_id="redshift_tc_dw",
    s3_conn_id="s3_sfdc_import_staging",
    provide_context=True,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

task_tli_2 = S3CopyOperator(
    task_id="copy_tli_2",
    schema="tc_datawarehouse",
    table="sas.tli_categorized_bucket",
    path="tc-datalake/sas/external_tables/tli/tli_categorized2.txt",
    delimiter="|",
    postgres_conn_id="redshift_tc_dw",
    s3_conn_id="s3_sfdc_import_staging",
    provide_context=True,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

task_tli_1 >> task_tli_2 >> check_drop("tli_categorized") >> swap_table(
    "tli_categorized"
)
for sobject in sobjects:
    copy_table(sobject) >> check_drop(sobject) >> swap_table(sobject)
