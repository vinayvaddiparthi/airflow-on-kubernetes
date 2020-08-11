from datetime import datetime
import json
import os
import sqlite3

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlalchemy as sql

from equifax_extras.models import Address, AddressRelationship, Applicant, ApplicantAttribute

from equifax_extras.consumer.request_file import RequestFile


def get_snowflake_engine(snowflake_connection, engine_kwargs=None):
    if engine_kwargs is None:
        engine_kwargs = dict()
    engine = SnowflakeHook(snowflake_connection, role='DBT_DEVELOPMENT', database='ZETATANGO', schema='KYC_STAGING').get_sqlalchemy_engine(engine_kwargs)
    return engine


def get_local_snowflake_engine(snowflake_connection):
    kwargs = {
        'connect_args': {"authenticator": "externalbrowser"}
    }
    engine = get_snowflake_engine(snowflake_connection, kwargs)
    return engine


def connect(engine):
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    return session


cwd = os.getcwd()
tmp = os.path.join(cwd, 'tmp')
if not os.path.exists(tmp):
    os.mkdir(tmp)

sqlite_db_path = os.path.join(tmp, 'test_database.db')
sqlite_db_url = f"sqlite:///{sqlite_db_path}"


def init_sqlite():
    conn = None
    try:
        conn = sqlite3.connect(sqlite_db_path)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def load_table(engine, table):
    metadata = sql.MetaData()
    t = sql.Table(table, metadata, autoload=True, autoload_with=engine)
    return t


def fetch_all(engine, table_name):
    table = load_table(engine, table_name)
    query = sql.select([table])
    connection = engine.connect()
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    return result_set


def load_addresses(engine):
    result_set = fetch_all(engine, 'ADDRESSES')

    sqlite_engine = create_engine(sqlite_db_url)

    if sqlite_engine.dialect.has_table(sqlite_engine, Address.__tablename__):
        Address.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, Address.__tablename__):
        Address.__table__.create(bind=sqlite_engine)
        session = connect(sqlite_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = Address(**d)
            session.add(record)
        session.commit()


def load_address_relationships(engine):
    result_set = fetch_all(engine, 'ADDRESS_RELATIONSHIPS')

    sqlite_engine = create_engine(sqlite_db_url)

    if sqlite_engine.dialect.has_table(sqlite_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.create(bind=sqlite_engine)
        session = connect(sqlite_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            if d['party_type'] == "Individuals::Applicant":
                d['party_type'] = "Applicant"
            if d['party_type'] == "Entities::Merchant":
                d['party_type'] = "Merchant"
            record = AddressRelationship(**d)
            session.add(record)
        session.commit()


def load_applicants(engine):
    result_set = fetch_all(engine, 'INDIVIDUALS_APPLICANTS')

    sqlite_engine = create_engine(sqlite_db_url)

    if sqlite_engine.dialect.has_table(sqlite_engine, Applicant.__tablename__):
        Applicant.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, Applicant.__tablename__):
        Applicant.__table__.create(bind=sqlite_engine)
        session = connect(sqlite_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = Applicant(**d)
            session.add(record)
        session.commit()


def load_applicant_attributes(engine):
    result_set = fetch_all(engine, 'INDIVIDUAL_ATTRIBUTES')

    sqlite_engine = create_engine(sqlite_db_url)

    if sqlite_engine.dialect.has_table(sqlite_engine, ApplicantAttribute.__tablename__):
        ApplicantAttribute.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, ApplicantAttribute.__tablename__):
        ApplicantAttribute.__table__.create(bind=sqlite_engine)
        session = connect(sqlite_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            # Delete deprecated individual_id if present
            d.pop('individual_id', None)
            applicant_id = d.pop('individuals_applicant_id', None)
            d['applicant_id'] = applicant_id
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = ApplicantAttribute(**d)
            session.add(record)
        session.commit()


def build():
    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    r = RequestFile("tmp/request_file.txt")
    applicants = session.query(Applicant).limit(50).all()
    for applicant in applicants:
        r.append(applicant)
    r.export()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1, 10, 00, 00),
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
    init_sqlite()

    e = get_local_snowflake_engine('snowflake_conn')
    load_applicants(e)
    load_applicant_attributes(e)
    load_addresses(e)
    load_address_relationships(e)
    build()
