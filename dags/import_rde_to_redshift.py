from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pendulum
from sqlalchemy import create_engine


def run_presto_query(query: str):
    engine = create_engine("presto://presto-production-internal.presto.svc:8080/rde")

    with engine.begin() as tx:
        tx.execute(query)


with DAG(
    "rde_prod_to_redshift",
    start_date=pendulum.datetime(
        2019, 10, 15, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="5 4 * * *",
    catchup=False,
) as dag:
    for table in ["assignment_history", "assignment_history_api"]:
        PythonOperator(
            task_id=f"presto_{table}",
            python_callable=run_presto_query,
            op_args=[
                f"""
                insert into glue2.risk_decision_engine.{table}
                select t1.* from rde."risk-decision-engine".{table} t1
                left join glue2.risk_decision_engine.{table} t2 on t1.account_uuid = t2.account_uuid and t1.created_date = t2.created_date
                where t2.account_uuid is null and t2.created_date is null
                """
            ],
        ) >> PostgresOperator(
            task_id=f"rs_delete_{table}",
            sql=f'DELETE FROM "rde"."{table}"',
            postgres_conn_id="redshift_tc_dw",
        ) >> PostgresOperator(
            task_id=f"rs_insert_{table}",
            postgres_conn_id="redshift_tc_dw",
            sql=f"""
                insert into rde.{table}
                select * from risk_decision_engine.{table}
                """,
        )

    for table in [
        "pricing_history",
        "pricing_history_api",
        "pricing_metadata",
        "pricing_metadata_api",
    ]:
        PythonOperator(
            task_id=f"presto_{table}",
            python_callable=run_presto_query,
            op_args=[
                f"""
                insert into glue2.risk_decision_engine.{table}
                select t1.* from rde."risk-decision-engine".{table} t1
                left join glue2.risk_decision_engine.{table} t2 on t1.pricing_uuid = t2.pricing_uuid
                where t2.pricing_uuid is null
                """
            ],
        ) >> PostgresOperator(
            task_id=f"rs_delete_{table}",
            sql=f'DELETE FROM "rde"."{table}"',
            postgres_conn_id="redshift_tc_dw",
        ) >> PostgresOperator(
            task_id=f"rs_insert_{table}",
            postgres_conn_id="redshift_tc_dw",
            sql=f"""
                insert into rde.{table}
                select * from risk_decision_engine.{table}
                """,
        )
