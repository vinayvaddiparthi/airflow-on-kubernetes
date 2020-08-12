from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker

import json
from datetime import datetime


def get_engine(snowflake_connection, snowflake_kwargs=None, engine_kwargs=None):
    if snowflake_kwargs is None:
        snowflake_kwargs = dict()
    if engine_kwargs is None:
        engine_kwargs = dict()
    engine = SnowflakeHook(
        snowflake_connection, **snowflake_kwargs
    ).get_sqlalchemy_engine(engine_kwargs)
    return engine


def get_local_engine(snowflake_connection):
    snowflake_kwargs = {
        'role': 'DBT_DEVELOPMENT',
        'database': 'ZETATANGO',
        'schema': 'KYC_STAGING',
    }
    engine_kwargs = {'connect_args': {'authenticator': 'externalbrowser'}}
    engine = get_engine(
        snowflake_connection,
        snowflake_kwargs=snowflake_kwargs,
        engine_kwargs=engine_kwargs,
    )
    return engine


def connect(engine):
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    return session


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


def load_addresses(remote_engine, local_engine):
    from equifax_extras.models import Address

    result_set = fetch_all(remote_engine, 'ADDRESSES')

    if local_engine.dialect.has_table(local_engine, Address.__tablename__):
        Address.__table__.drop(bind=local_engine)

    if not local_engine.dialect.has_table(local_engine, Address.__tablename__):
        Address.__table__.create(bind=local_engine)
        session = connect(local_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = Address(**d)
            session.add(record)
        session.commit()


def load_address_relationships(remote_engine, local_engine):
    from equifax_extras.models import AddressRelationship

    result_set = fetch_all(remote_engine, 'ADDRESS_RELATIONSHIPS')

    if local_engine.dialect.has_table(local_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.drop(bind=local_engine)

    if not local_engine.dialect.has_table(local_engine, AddressRelationship.__tablename__):
        AddressRelationship.__table__.create(bind=local_engine)
        session = connect(local_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            if d['party_type'] == 'Individuals::Applicant':
                d['party_type'] = 'Applicant'
            if d['party_type'] == 'Entities::Merchant':
                d['party_type'] = 'Merchant'
            record = AddressRelationship(**d)
            session.add(record)
        session.commit()


def load_applicants(remote_engine, local_engine):
    from equifax_extras.models import Applicant

    result_set = fetch_all(remote_engine, 'INDIVIDUALS_APPLICANTS')

    if local_engine.dialect.has_table(local_engine, Applicant.__tablename__):
        Applicant.__table__.drop(bind=local_engine)

    if not local_engine.dialect.has_table(local_engine, Applicant.__tablename__):
        Applicant.__table__.create(bind=local_engine)
        session = connect(local_engine)
        for result in result_set:
            result_json = result.values()[0]
            d = json.loads(result_json)
            d['created_at'] = datetime.strptime(d['created_at'], '%Y-%m-%d %H:%M:%S.%f')
            d['updated_at'] = datetime.strptime(d['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
            record = Applicant(**d)
            session.add(record)
        session.commit()


def load_applicant_attributes(remote_engine, local_engine):
    from equifax_extras.models import ApplicantAttribute

    result_set = fetch_all(remote_engine, 'INDIVIDUAL_ATTRIBUTES')

    if local_engine.dialect.has_table(local_engine, ApplicantAttribute.__tablename__):
        ApplicantAttribute.__table__.drop(bind=local_engine)

    if not local_engine.dialect.has_table(local_engine, ApplicantAttribute.__tablename__):
        ApplicantAttribute.__table__.create(bind=local_engine)
        session = connect(local_engine)
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
