from datetime import datetime

import os

import json

import sqlite3

from importlib import import_module

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine

from utils.sqlalchemy_table import Base, build_sqlalchemy_descriptor, define_table
import sqlalchemy as sql

from equifax_extras.commercial.commercial import dt_comm, dt_tcap
from equifax_extras.models.applicant import Applicant
from equifax_extras.models.address import Address
from equifax_extras.models.address_relationship import AddressRelationship
from equifax_extras.models.encryption_key import EncryptionKey


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


def load_table(engine, table):
    metadata = sql.MetaData()
    t = sql.Table(table, metadata, autoload=True, autoload_with=engine)
    return t


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


def load_addresses(snowflake_conn):
    engine = get_local_snowflake_engine(snowflake_conn)
    t = load_table(engine, 'ADDRESSES')
    query = sql.select([t])
    connection = engine.connect()
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    results = (s for s in result_set)
    results_json = (r.values()[0] for r in results)

    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    if sqlite_engine.dialect.has_table(sqlite_engine, Address.__tablename__):
        Address.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, Address.__tablename__):
        Address.__table__.create(bind=sqlite_engine)
        for j in results_json:
            d = json.loads(j)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = Address(**d)
            session.add(record)
        session.commit()


def load_address_relationships(snowflake_conn):
    engine = get_local_snowflake_engine(snowflake_conn)
    t = load_table(engine, 'ADDRESS_RELATIONSHIPS')
    query = sql.select([t])
    connection = engine.connect()
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    results = (s for s in result_set)
    results_json = (r.values()[0] for r in results)

    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    if sqlite_engine.dialect.has_table(sqlite_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.create(bind=sqlite_engine)
        for j in results_json:
            d = json.loads(j)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            if d['party_type'] == "Individuals::Applicant":
                d['party_type'] = "Applicant"
            if d['party_type'] == "Entities::Merchant":
                d['party_type'] = "Merchant"
            record = AddressRelationship(**d)
            session.add(record)
        session.commit()


def load_applicants(snowflake_conn):
    engine = get_local_snowflake_engine(snowflake_conn)
    t = load_table(engine, 'INDIVIDUALS_APPLICANTS')
    query = sql.select([t])
    connection = engine.connect()
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    results = (s for s in result_set)
    results_json = (r.values()[0] for r in results)

    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    if sqlite_engine.dialect.has_table(sqlite_engine, Applicant.__tablename__):
        Applicant.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, Applicant.__tablename__):
        Applicant.__table__.create(bind=sqlite_engine)
        for j in results_json:
            d = json.loads(j)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            applicant = Applicant(**d)
            session.add(applicant)
        session.commit()


def load_encryption_keys(snowflake_conn):
    engine = get_local_snowflake_engine(snowflake_conn)
    t = load_table(engine, 'ENCRYPTION_KEYS')
    query = sql.select([t])
    connection = engine.connect()
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    results = (s for s in result_set)
    results_json = (r.values()[0] for r in results)

    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    if sqlite_engine.dialect.has_table(sqlite_engine, EncryptionKey.__tablename__):
        EncryptionKey.__table__.drop(bind=sqlite_engine)

    if not sqlite_engine.dialect.has_table(sqlite_engine, EncryptionKey.__tablename__):
        EncryptionKey.__table__.create(bind=sqlite_engine)
        for j in results_json:
            d = json.loads(j)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = EncryptionKey(**d)
            session.add(record)
        session.commit()


def build():
    sqlite_engine = create_engine(sqlite_db_url)
    session = connect(sqlite_engine)

    applicant = session.query(Applicant).first()
    # print(applicant.date_of_birth)
    # print(applicant.first_name)
    # print(applicant.middle_name)
    # print(applicant.last_name)
    print("------------------------------------DEBUG")
    print(applicant.addresses)
    print(applicant.physical_address)
    print(applicant.legal_business_address)
    print("------------------------------------DEBUG")


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
    # load_applicants('snowflake_conn')
    # load_encryption_keys('snowflake_conn')
    # load_addresses('snowflake_conn')
    # load_address_relationships('snowflake_conn')
    build()
