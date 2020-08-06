import datetime as dt

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.orm import sessionmaker

from utils.sqlalchemy_table import Base, build_sqlalchemy_descriptor, define_table

from equifax_extras.commercial.commercial import dt_comm, dt_tcap


def get_snowflake_engine(snowflake_connection, engine_kwargs=None):
    if engine_kwargs is None:
        engine_kwargs = dict()
    engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine(engine_kwargs)
    return engine


def get_local_snowflake_engine(snowflake_connection):
    kwargs = {
        'connect_args': {"authenticator": "externalbrowser"}
    }
    engine = get_snowflake_engine(snowflake_connection, kwargs)
    return engine


def connect_snowflake(engine):
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    return session


def build(snowflake_conn):
    tables = [
        {
            'name': 'equifax_comm_2020_08_05',
            'descriptor': build_sqlalchemy_descriptor(dt_comm)
        },
        {
            'name': 'equifax_tcap_2020_08_05',
            'descriptor': build_sqlalchemy_descriptor(dt_tcap)
        }
    ]

    for table in tables:
        table_name = table['name']
        descriptor = table['descriptor']
        define_table(table_name, 'equifax', descriptor)

    engine = get_local_snowflake_engine(snowflake_conn)
    session = connect_snowflake(engine)
    Base.metadata.create_all(engine)

    # row = TestTable(customer_ref_no='abc', business_name='MY BUSINESS', city='Toronto')
    # session.add(row)
    session.commit()


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 1, 1, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG(
    'test_dag',
    catchup=False,
    default_args=default_args,
    schedule_interval='@once',
) as dag:
    opr_func = PythonOperator(
        task_id=f"test_connection",
        python_callable=build,
        op_kwargs={
            "snowflake_conn": "snowflake_conn",
        },
    )

if __name__ == "__main__":
    build("snowflake_conn")
