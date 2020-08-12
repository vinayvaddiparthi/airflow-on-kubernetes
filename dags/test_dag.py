from datetime import datetime
import os
import sqlite3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from equifax_extras.models import Applicant
from equifax_extras.consumer import RequestFile
import equifax_extras.utils.snowflake as snowflake


cwd = os.getcwd()
tmp = os.path.join(cwd, "tmp")
if not os.path.exists(tmp):
    os.mkdir(tmp)

sqlite_db_path = os.path.join(tmp, "test_database.db")
sqlite_db_url = f"sqlite:///{sqlite_db_path}"


def init_sqlite():
    print(f"Connecting to {sqlite_db_url}")
    conn = None
    try:
        conn = sqlite3.connect(sqlite_db_path)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def generate_file():
    sqlite_engine = create_engine(sqlite_db_url)
    session_maker = sessionmaker(bind=sqlite_engine)
    session = session_maker()

    r = RequestFile("tmp/request_file.txt")
    applicants = session.query(Applicant).limit(50).all()
    for applicant in applicants:
        r.append(applicant)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1, 10, 00, 00),
    "concurrency": 1,
    "retries": 0,
}

environment = Variable.get("environment", "")
if environment == "development":
    snowflake_engine = snowflake.get_local_engine("snowflake_conn")
else:
    snowflake_engine = snowflake.get_engine("snowflake_conn")

with DAG(
    dag_id="generate_file",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    op_init_sqlite = PythonOperator(task_id="init_sqlite", python_callable=init_sqlite,)
    op_load_addresses = PythonOperator(
        task_id="load_addresses",
        python_callable=snowflake.load_addresses,
        op_kwargs={
            "remote_engine": snowflake_engine,
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_address_relationships = PythonOperator(
        task_id="load_address_relationships",
        python_callable=snowflake.load_address_relationships,
        op_kwargs={
            "remote_engine": snowflake_engine,
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_applicants = PythonOperator(
        task_id="load_applicants",
        python_callable=snowflake.load_applicants,
        op_kwargs={
            "remote_engine": snowflake_engine,
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_applicant_attributes = PythonOperator(
        task_id="load_applicant_attributes",
        python_callable=snowflake.load_applicant_attributes,
        op_kwargs={
            "remote_engine": snowflake_engine,
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_generate_file = PythonOperator(
        task_id="generate_file", python_callable=generate_file
    )

load = [
    op_load_addresses,
    op_load_address_relationships,
    op_load_applicants,
    op_load_applicant_attributes,
]
op_init_sqlite >> load >> op_generate_file


if __name__ == "__main__":
    init_sqlite()
    sqlite_engine = create_engine(sqlite_db_url)
    snowflake.load_applicants(snowflake_engine, sqlite_engine)
    snowflake.load_applicant_attributes(snowflake_engine, sqlite_engine)
    snowflake.load_addresses(snowflake_engine, sqlite_engine)
    snowflake.load_address_relationships(snowflake_engine, sqlite_engine)
    generate_file()
