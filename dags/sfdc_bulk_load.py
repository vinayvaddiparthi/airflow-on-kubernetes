import datetime
import itertools
import types
from concurrent import futures
from concurrent.futures._base import Executor
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
from airflow.operators.python_operator import PythonOperator
from pyarrow._csv import ParseOptions
from salesforce_bulk.bulk_states import NOT_PROCESSED
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce import Salesforce, SalesforceMalformedRequest
from salesforce_bulk import SalesforceBulk
from slugify import slugify
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

WIDE_THRESHOLD = 50
NUM_BUCKETS = 16


def _monkey_patched_get_query_batch_results(
    self, batch_id, result_id, job_id=None, chunk_size=2048, raw=False
):
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
    pk_chunking: bool = False,
) -> Iterator[requests.Response]:
    filters = stmt_filters(schema, sobject)

    if max_date:
        filters.append(
            f"{max_date_col} >= {max_date.replace(tzinfo=datetime.timezone.utc).isoformat()}"
        )

    stmt = f"select {','.join(fields)} from {sobject}" + (
        f" where {' and '.join(filters)}" if len(filters) > 0 else ""
    )

    print(stmt)

    if pk_chunking:
        for suffix in ["History", "Share"]:
            if sobject.endswith(suffix):
                if sobject.endswith("__History"):
                    parent = sobject.replace("__History", "__c")
                else:
                    parent = sobject[: -len(suffix)]

                pk_chunking = f"parent={parent}"

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
    destination_schema: str,
    destination_table: str,
    engine_: Engine,
    resps: Iterator[requests.Response],
):
    with engine_.begin() as tx:
        print(
            tx.execute(
                f"create stage if not exists {destination_schema}.{destination_table} "
                "file_format=(type=parquet)"
            ).fetchall()
        )  # nosec

    dt_suffix = slugify(pendulum.datetime.now().isoformat(), separator="_")
    with engine_.begin() as tx:
        for i, resp in enumerate(resps):
            with TemporaryDirectory() as tempdir:
                tempdir = Path(tempdir)
                json_filepath = (tempdir / destination_table).with_suffix(f".{i}.json")
                pq_filepath = (tempdir / destination_table).with_suffix(
                    f".{dt_suffix}.{i}.pq"
                )

                with open(json_filepath, "w+b") as csv_file:
                    for chunk in resp.iter_content(chunk_size=128):
                        csv_file.write(chunk)

                #  Convert to parquet to avoid newline shenanigans
                table = pv.read_csv(
                    f"{json_filepath}",
                    parse_options=ParseOptions(newlines_in_values=True),
                )
                pq.write_table(table, f"{pq_filepath}")

                print(
                    tx.execute(
                        f"put file://{pq_filepath} @{destination_schema}.{destination_table}"
                    ).fetchall()
                )

        with engine_.begin() as tx:
            print(
                tx.execute(
                    f"create or replace transient table {destination_schema}.{destination_table} as "  # nosec
                    f"select $1 as fields from @{destination_schema}.{destination_table} "
                ).fetchall()
            )


def process_sobject(
    sobject: SobjectDescriptor,
    bulk: SalesforceBulk,
    engine_: Engine,
    schema: str,
    pk_chunking: Union[bool, str] = False,
):
    print(f"Processing sobject {sobject.name}")

    max_date_col = (
        "SystemModstamp"
        if not (sobject.name.endswith("__History") or sobject.name.endswith("__Share"))
        else "CreatedDate"
    )
    max_date: Optional[datetime.datetime] = None

    if len(sobject.fields) >= WIDE_THRESHOLD:
        chunks: List[List[str]] = [["Id", max_date_col]] * NUM_BUCKETS
        for field in (
            field for field in sobject.fields if field not in {"Id", max_date_col}
        ):
            chunks[hash(field) % NUM_BUCKETS].append(field)
    else:
        chunks: List[List[str]] = [sobject.fields]

    for i, fields in enumerate(chunks):
        destination_table = f"{sobject.name}___PART_{i}"

        stmt = select(
            columns=[func.max(text(f'fields:"{max_date_col}"::datetime'))],
            from_obj=text(f"{schema}.{destination_table}"),
        )

        try:
            with engine_.begin() as tx:
                max_date = tx.execute(stmt).fetchall()[0][0]
        except DBAPIError as exc:
            print(f"Executing {stmt} raised \n{exc}")

        try:
            resps = get_resps_from_fields(
                sobject.name,
                fields,
                bulk,
                schema,
                pk_chunking=pk_chunking,
                max_date_col=max_date_col,
                max_date=max_date,
            )
            put_resps_on_snowflake(schema, destination_table, engine_, resps)
        except Exception as exc:
            print(f"put_resps_on_snowflake raised \n{exc}")


def get_sobjects(
    executor: Executor, salesforce: Salesforce
) -> Iterator[Union[SobjectDescriptor, SalesforceMalformedRequest]]:
    def _describe_sobjects(
        sobject: str,
    ) -> Union[SobjectDescriptor, SalesforceMalformedRequest]:
        try:
            print(f"Getting metadata for sobject {sobject}")
            return SobjectDescriptor(
                name=sobject,
                fields=[
                    field["name"]
                    for field in getattr(salesforce, sobject).describe()["fields"]
                    if field["type"] != "address"
                ],
                count=salesforce.query(f"select count(id) from {sobject}")[  # nosec
                    "records"
                ][0]["expr0"],
            )
        except SalesforceMalformedRequest as exc:
            return exc

    sobjects = (x for x in salesforce.describe()["sobjects"] if x["queryable"])

    futures = (
        executor.submit(_describe_sobjects, sobject["name"]) for sobject in sobjects
    )

    return (future.result() for future in futures)


def import_sfdc(snowflake_conn: str, salesforce_conn: str, schema: str):
    engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    salesforce_connection = BaseHook.get_connection(salesforce_conn)
    salesforce = Salesforce(
        username=salesforce_connection.login,
        password=salesforce_connection.password,
        security_token=salesforce_connection.extra_dejson["security_token"],
    )
    salesforce_bulk = SalesforceBulk(
        username=salesforce_connection.login,
        password=salesforce_connection.password,
        security_token=salesforce_connection.extra_dejson["security_token"],
    )

    with ThreadPoolExecutor(max_workers=4) as metadata_executor, ThreadPoolExecutor(
        max_workers=4
    ) as processing_executor:
        futures_ = [
            process_sobject(
                sobject,
                salesforce_bulk,
                engine_,
                schema,
                pk_chunking=sobject.count > 2_000_000,
            )
            for sobject in get_sobjects(metadata_executor, salesforce)
            if not isinstance(sobject, Exception) and not sobject.count == 0
        ]

        futures.wait(futures_)


def create_dag(instances: List[str]) -> DAG:
    with DAG(
        f"salesforce_bulk_import",
        start_date=pendulum.datetime(
            2020, 6, 21, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 0 * * *",
        catchup=False,
    ) as dag:
        for instance in instances:
            dag << PythonOperator(
                task_id=f"import_{instance}",
                python_callable=import_sfdc,
                op_kwargs={
                    "snowflake_conn": "snowflake_salesforce2",
                    "salesforce_conn": f"salesforce_{instance}",
                    "schema": instance,
                },
                retry_delay=datetime.timedelta(hours=1),
                retries=3,
                executor_config={
                    "resources": {"request_memory": "8Gi", "limit_memory": "8Gi"},
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

    url = (
        URL(account=account, database=database, role=role)
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
                "authenticator": "oauth",
                "token": os.environ.get("ACCESS_TOKEN"),
            },
        ),
    ) as mock_engine:
        import_sfdc("snowflake_conn", "salesforce_conn", "sfoi")
else:
    globals()[f"salesforce_bulk_import_dag"] = create_dag(["sfoi", "sfni"])
