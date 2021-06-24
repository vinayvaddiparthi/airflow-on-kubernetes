import datetime
import itertools
import types
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union, Callable, Iterator, List, Optional

import attr
import pendulum
import pyarrow.csv as pv, pyarrow.parquet as pq
import requests
import urllib.parse as urlparse

from airflow import DAG

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pyarrow._csv import ParseOptions
from salesforce_bulk.bulk_states import NOT_PROCESSED
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce import Salesforce, SalesforceMalformedRequest
from salesforce_bulk import SalesforceBulk
from slugify import slugify
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine

from utils.failure_callbacks import slack_dag

PK_CHUNKING_THRESHOLD = 2_000_000
WIDE_THRESHOLD = 200
NUM_BUCKETS = 16


def _monkey_patched_get_query_batch_results(
    self: SalesforceBulk,
    batch_id: str,
    result_id: str,
    job_id: Optional[str] = None,
    chunk_size: int = 2048,
    raw: bool = False,
) -> requests.Response:
    job_id = job_id or self.lookup_job_id(batch_id)

    uri = urlparse.urljoin(
        self.endpoint + "/",
        "job/{0}/batch/{1}/result/{2}".format(job_id, batch_id, result_id),
    )

    resp = requests.get(uri, headers=self.headers(), stream=True)
    self.check_status(resp)
    return resp


@attr.s
class SobjectDescriptor:
    name: str = attr.ib()
    fields: List[str] = attr.ib()
    count: int = attr.ib()


@attr.s
class SobjectBucket:
    name: str = attr.ib()
    bucketing_callable: Callable = attr.ib()
    processing_callable: Callable = attr.ib()
    sobjects: List[SobjectDescriptor] = attr.ib(default=attr.Factory(list))


def stmt_filters(schema: str, sobject_name: str) -> List[str]:
    filters = {"sfoi": {"Account": ["Test_Account__c = FALSE"]}}.get(schema, {})
    return filters[sobject_name] if sobject_name in filters else []


def get_resps_from_fields(
    sobject: str,
    fields: List[str],
    bulk: SalesforceBulk,
    schema: str,
    max_date_col: str,
    max_date: Optional[datetime.datetime] = None,
    pk_chunking: Union[bool, str, int] = False,
) -> Iterator[requests.Response]:
    filters = stmt_filters(schema, sobject)

    if max_date:
        filters.append(f"{max_date_col} > {max_date.isoformat()}")

    stmt = f"select {','.join(fields)} from {sobject}" + (
        f" where {' and '.join(filters)}" if len(filters) > 0 else ""
    )

    print(stmt)

    if pk_chunking:
        for suffix, parent in [
            ("__History", sobject.replace("__History", "__c")),
            ("__Share", sobject.replace("__Share", "__c")),
            ("FieldHistory", sobject[: -len("FieldHistory")]),
            ("History", sobject[: -len("History")]),
            ("Share", sobject[: -len("Share")]),
        ]:
            if sobject.endswith(suffix):
                pk_chunking = f"chunkSize=250000;parent={parent}"
                break
        else:
            pk_chunking = 250000

    try:
        job_id = bulk.create_queryall_job(
            sobject, contentType="CSV", pk_chunking=pk_chunking
        )
        batch = bulk.query(job_id, stmt)

        try:
            bulk.wait_for_batch(job_id, batch, timeout=3600)
        except BulkBatchFailed as exc:
            if pk_chunking and exc.state == NOT_PROCESSED:
                pass
            else:
                raise exc
    finally:
        if "job_id" in locals() and job_id:
            bulk.close_job(job_id)

    batches = bulk.get_batch_list(job_id)

    resps: List[Iterator] = []
    for batch in batches:
        batch_id = batch["id"]

        try:
            bulk.wait_for_batch(job_id, batch_id, timeout=3600)
        except BulkBatchFailed as exc:
            if pk_chunking and exc.state == NOT_PROCESSED:
                continue
            else:
                raise exc

        result_ids = bulk.get_query_batch_result_ids(batch_id, job_id=job_id)

        if not result_ids:
            raise RuntimeError("Batch is not complete")

        bulk.get_query_batch_results = types.MethodType(
            _monkey_patched_get_query_batch_results, bulk
        )

        resps.append(
            (
                bulk.get_query_batch_results(
                    batch_id=batch_id, result_id=result_id, job_id=job_id
                )
                for result_id in result_ids
            )
        )

    return itertools.chain.from_iterable(resps)


def put_resps_on_snowflake(
    destination_database: str,
    destination_schema: str,
    destination_table: str,
    engine_: Engine,
    resps: Iterator[requests.Response],
) -> None:
    dt_suffix = slugify(pendulum.datetime.now().isoformat(), separator="_")
    with engine_.begin() as tx:
        for i, resp in enumerate(resps):
            with TemporaryDirectory() as tempdir:
                json_filepath = Path(tempdir, destination_table).with_suffix(
                    f".{i}.json"
                )
                pq_filepath = Path(tempdir, destination_table).with_suffix(
                    f".{dt_suffix}.{i}.pq"
                )

                with open(json_filepath, "w+b") as csv_file:
                    for chunk in resp.iter_content(chunk_size=128):
                        csv_file.write(chunk)

                #  Convert to parquet to avoid newline shenanigans in source CSV
                table = pv.read_csv(
                    f"{json_filepath}",
                    parse_options=ParseOptions(newlines_in_values=True),
                )
                pq.write_table(table, f"{pq_filepath}")

                print(
                    tx.execute(
                        f"put file://{pq_filepath} @{destination_database}.{destination_schema}.{destination_table}"
                    ).fetchall()
                )


def describe_sobject(
    salesforce: Salesforce,
    sobject_name: str,
) -> Union[SobjectDescriptor, SalesforceMalformedRequest]:
    print(f"Getting metadata for sobject {sobject_name}")
    return SobjectDescriptor(
        name=sobject_name,
        fields=[
            field["name"]
            for field in getattr(salesforce, sobject_name).describe()["fields"]
            if field["type"] != "address"
        ],
        count=salesforce.query(f"select count(Id) from {sobject_name}")[  # nosec
            "records"
        ][0]["expr0"],
    )


def ensure_stage_and_view(
    engine_: Engine,
    destination_database: str,
    destination_schema: str,
    destination_table: str,
) -> None:
    with engine_.begin() as tx:
        stmts = [
            f"use database {destination_database}",
            f"create stage if not exists {destination_schema}.{destination_table} "  # nosec
            f"  file_format=(type=parquet)",  # nosec
            f"create or replace view {destination_schema}.{destination_table} as "  # nosec
            f"  select $1 as fields from @{destination_schema}.{destination_table}",  # nosec
        ]

        print([tx.execute(stmt).fetchall() for stmt in stmts])  # nosec


def process_sobject(
    sobject_name: str,
    salesforce: Salesforce,
    bulk: SalesforceBulk,
    engine_: Engine,
    database: str,
    schema: str,
) -> None:
    try:
        sobject = describe_sobject(salesforce, sobject_name)
    except SalesforceMalformedRequest as exc:
        print(f"⚠️Skipping {sobject_name} because describe_sobject raised {exc}")
        return

    if sobject.count == 0:
        print(f"⚠️Skipping {sobject_name} because it is empty")
        return

    print(f"⚙️Processing sobject {sobject.name}")

    for suffix, max_date_col in [
        ("History", "CreatedDate"),
        ("Share", "LastModifiedDate"),
    ]:
        if sobject.name.endswith(suffix):
            break
    else:
        max_date_col = "SystemModstamp"

    if "SystemModstamp" not in sobject.fields:
        if "LastModifiedDate" in sobject.fields:
            max_date_col = "LastModifiedDate"
        else:
            max_date_col = "CreatedDate"

    chunks: List[List[str]] = [sobject.fields]
    if len(sobject.fields) >= WIDE_THRESHOLD:
        chunks = [["Id", max_date_col] for _ in range(NUM_BUCKETS)]
        for field in (
            field for field in sobject.fields if field not in {"Id", max_date_col}
        ):
            i = hash(field) % NUM_BUCKETS
            chunks[i].append(field)

    with engine_.begin() as create_schema:
        stmts = [
            f"use database {database}",
            f"create schema if not exists {schema}"
        ]
        print([create_schema.execute(stmt).fetchall() for stmt in stmts])

    for i, fields in enumerate(chunks):
        destination_table = f"{sobject.name}___PART_{i}"
        ensure_stage_and_view(engine_, database, schema, destination_table)

        stmt = select(
            columns=[func.max(text(f'fields:"{max_date_col}"::datetime'))],
            from_obj=text(f"{schema}.{destination_table}"),
        )

        max_date: Optional[datetime.datetime] = None
        try:
            with engine_.begin() as tx:
                tx.execute(f"use database {database}").fetchall()
                max_date = (
                    tx.execute(stmt)
                    .fetchall()[0][0]
                    .replace(tzinfo=datetime.timezone.utc)
                )

            if max_date is not None:
                num_recs_to_load = salesforce.query(
                    f"select count(Id) from {sobject.name} "  # nosec
                    f"where {max_date_col} > {max_date.isoformat()}"  # nosec
                )["records"][0]["expr0"]
            else:
                raise Exception("max_date was None")

        except Exception as exc:
            print(
                f"📝️{stmt} raised {exc}; setting max_date to None "
                f"and num_recs_to_load to sobject.count"
            )
            num_recs_to_load = sobject.count

        if num_recs_to_load == 0:
            print(f"📝️Skipping {destination_table} because there are no new records")
            continue

        try:
            resps = get_resps_from_fields(
                sobject.name,
                fields,
                bulk,
                schema,
                pk_chunking=num_recs_to_load > PK_CHUNKING_THRESHOLD,
                max_date_col=max_date_col,
                max_date=max_date,
            )
            put_resps_on_snowflake(database, schema, destination_table, engine_, resps)
        except Exception as exc:
            print(f"❌️put_resps_on_snowflake on {destination_table} raised \n{exc}")

        print(f"✨️Done processing {sobject.name}")


def import_sfdc(
    snowflake_conn: str, salesforce_conn: str, database: str, schema: str
) -> None:
    engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    salesforce_connection = BaseHook.get_connection(salesforce_conn)
    salesforce = Salesforce(
        username=salesforce_connection.login,
        password=salesforce_connection.password,
        security_token=salesforce_connection.extra_dejson["security_token"],
        domain="test",  # need to be removed for prod
    )
    salesforce_bulk = SalesforceBulk(
        username=salesforce_connection.login,
        password=salesforce_connection.password,
        security_token=salesforce_connection.extra_dejson["security_token"],
        domain="test",  # need to be removed for prod
    )

    with ThreadPoolExecutor(max_workers=4) as processing_executor:
        futures_ = [
            processing_executor.submit(
                process_sobject,
                sobject_name,
                salesforce,
                salesforce_bulk,
                engine_,
                database,
                schema,
            )
            for sobject_name in (
                x["name"] for x in salesforce.describe()["sobjects"] if x["queryable"]
            )
        ]

        futures.wait(futures_)


def create_dag(instances: List[str]) -> DAG:
    with DAG(
        "salesforce_bulk_import",
        start_date=pendulum.datetime(
            2020, 6, 21, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 0 * * *",
        catchup=False,
        description="",
        on_failure_callback=slack_dag("slack_data_alerts"),
        max_active_runs=1,
    ) as dag:
        for instance in instances:
            dag << PythonOperator(
                task_id=f"import_{instance}",
                python_callable=import_sfdc,
                op_kwargs={
                    "snowflake_conn": "airflow_production",
                    "salesforce_conn": f"salesforce_{instance}_sandbox",
                    "database": "salesforce2",
                    "schema": instance,
                },
                pool="sfdc_pool",
                retry_delay=datetime.timedelta(hours=1),
                retries=3,
                executor_config={
                    "resources": {
                        "requests": {"memory": "8Gi"},
                    },
                },
            )

        return dag


if __name__ == "__main__":
    import os
    from unittest.mock import MagicMock, patch
    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "thinkingcapital.ca-central-1.aws")
    database = os.environ.get("SNOWFLAKE_DATABASE", "SALESFORCE2")
    role = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
    user = os.environ.get("SNOWFLAKE_USERNAME")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    url = (
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse="ETL",
            role=role,
        )
        if role
        else URL(account=account, database=database)
    )

    mock = MagicMock()
    mock.login = os.environ.get("SALESFORCE_USERNAME")
    mock.password = os.environ.get("SALESFORCE_PASSWORD")
    mock.extra_dejson = {"security_token": os.environ.get("SALESFORCE_TOKEN")}

    with patch(
        "dags.sfdc_bulk_load.BaseHook.get_connection", return_value=mock
    ) as mock_conn, patch(
        "dags.sfdc_bulk_load.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            url,
            connect_args={
                "authenticator": "externalbrowser",
            },
        ),
    ) as mock_engine:
        import_sfdc("snowflake_conn", "salesforce_conn", database, "zetatango")
else:
    globals()["salesforce_bulk_import_dag"] = create_dag(
        Variable.get("salesforce_instances", deserialize_json=True)
    )
