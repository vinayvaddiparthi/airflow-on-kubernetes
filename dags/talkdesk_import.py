import datetime
import json
import logging
import os
from time import sleep
from typing import Any, Optional, Dict

import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from authlib.integrations.requests_client import OAuth2Session
from fs_s3fs import S3FS
from retrying import retry
from sqlalchemy import (
    create_engine,
    func,
    Table,
    MetaData,
    Column,
    literal_column,
    cast,
    VARCHAR,
)
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select
from snowflake.sqlalchemy import VARIANT

MAX_RUNS = 360


def import_talkdesk(
    snowflake_conn: str,
    talkdesk_conn: str,
    schema: str,
    bucket_name: str,
    execution_date: pendulum.datetime,
    next_execution_date: pendulum.datetime,
    cmk_key_id: str,
    **kwargs: Any,
) -> None:
    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    taskdesk_connection = BaseHook.get_connection(talkdesk_conn)

    metadata = MetaData()
    engine: Engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()

    t = Table(
        "calls",
        metadata,
        Column("call", VARIANT),
        Column("recordings", VARIANT),
        schema=schema,
    )
    t.create(engine, checkfirst=True)

    oauth_args = {
        "client_id": taskdesk_connection.login,
        "client_secret": taskdesk_connection.password,
        "scope": "reports:read reports:write recordings:read",
        "token_endpoint": taskdesk_connection.host,
        "grant_type": "client_credentials",
    }

    for i in range((next_execution_date - execution_date).days):
        start_dt = execution_date + datetime.timedelta(days=i)
        end_dt = start_dt + datetime.timedelta(days=1)
        fetch_calls_for_date_range(bucket_name, end_dt, engine, oauth_args, start_dt, t)
        logging.info(f"✨ Done importing calls from {start_dt} to {end_dt}")


@retry(stop_max_attempt_number=3)
def fetch_calls_for_date_range(
    bucket_name: str,
    end_dt: pendulum.datetime,
    engine: Engine,
    oauth_args: Dict,
    start_dt: pendulum.datetime,
    t: Table,
) -> None:

    logging.info(f"⚙ Importing calls from {start_dt} to {end_dt}")

    with OAuth2Session(**oauth_args) as session:
        token = session.fetch_token()

        initial_payload = {
            "format": "json",
            "timespan": {"from": start_dt.isoformat(), "to": end_dt.isoformat(),},
        }

        resp = session.post(
            "https://api.talkdeskapp.com/reports/calls/jobs", json=initial_payload,
        )

        run: int = 0
        while (status := (data := resp.json()).get("status")) not in [
            None,
            "done",
            "failed",
        ] and run < MAX_RUNS:
            logging.info(
                f"💤 Sleeping for 10 seconds because status is {status} ({run})"
            )
            sleep(10)
            resp = session.get(
                data["_links"]["self"]["href"]
            )  # Will be autoredirected to the report file

            run += 1

        logging.debug(data)

        if status == "failed":
            raise Exception(resp.text)

        fs = S3FS(bucket_name)

        entries = data.get("entries", [])

        with engine.begin() as tx:
            ids = [entry.get("call_id") for entry in entries]
            stmt = Select(
                columns=[cast(literal_column("call:call_id"), VARCHAR)],
                from_obj=t,
                whereclause=literal_column("call:call_id").in_(ids),
            )
            ids_in_db = [row[0] for row in tx.execute(stmt).fetchall()]

        logging.info(
            f"entries list contains {len(entries)} records of which {len(ids_in_db)} are already imported"
        )

    for entry in [
        entry for entry in entries if entry.get("call_id") not in ids_in_db
    ]:  # for each entry in the calls report
        with OAuth2Session(**oauth_args, token=token) as session, engine.begin() as tx:
            call_id: str = entry["call_id"]

            # get the recordings report associated with the entry
            recordings: Optional[Dict] = (
                session.get(recording_url).json()
                if (recording_url := entry.get("recording_url"))
                else None
            )

            logging.debug(f"call id: {call_id} recording: {recordings}")

            if recordings and (embedded := recordings.get("_embedded")):
                # download all recordings and re-upload them to S3
                for recording in embedded.get("recordings", []):
                    fp = f"{recording['id']}.mp3"
                    media_link = recording["_links"]["media"]["href"]
                    fs.writebytes(fp, session.get(media_link).content)

                    logging.info(f"☎ Uploaded recording {fp} to S3")
            else:
                logging.info(f"⚠ Could not find recording_url key in {call_id}")

            # insert call and recordings metadata in Snowflake
            selectable = Select(
                [
                    func.parse_json(json.dumps(entry)).label("call"),
                    func.parse_json(json.dumps(recordings)).label("recordings"),
                ]
            )
            tx.execute(t.insert().from_select(["call", "recordings"], selectable))

            token = session.token


def create_dag() -> DAG:
    with DAG(
        "talkdesk_import_monthly",
        start_date=pendulum.datetime(
            2015, 1, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=datetime.timedelta(weeks=4),
        catchup=True,
        max_active_runs=4,
    ) as dag:
        dag << PythonOperator(
            task_id="talkdesk_import",
            python_callable=import_talkdesk,
            op_kwargs={
                "snowflake_conn": "snowflake_talkdesk",
                "talkdesk_conn": "http_talkdesk",
                "schema": "TALKDESK",
                "bucket_name": "tc-talkdesk",
                "cmk_key_id": "04bc297e-6ec2-4fa0-b3aa-ffb29d40f306",
            },
            provide_context=True,
            retry_delay=datetime.timedelta(minutes=5),
            retries=5,
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowProductionTalkdeskRole"
                    },
                },
                "resources": {"request_memory": "512Mi"},
            },
        )

        return dag


if __name__ == "__main__":
    from snowflake.sqlalchemy import URL
    from unittest.mock import patch, MagicMock

    mock = MagicMock()
    mock.login = os.environ.get("CLIENT_ID")
    mock.password = os.environ.get("CLIENT_SECRET")
    mock.host = os.environ.get("TOKEN_ENDPOINT")
    mock.extra_dejson = {
        "token_endpoint": os.environ.get("TOKEN_ENDPOINT"),
    }

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "thinkingcapital.ca-central-1.aws")
    database = os.environ.get("SNOWFLAKE_DATABASE", "TCLEGACY")
    role = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
    user = os.environ.get("SNOWFLAKE_USER")

    url = (
        URL(account=account, database=database, role=role, user=user)
        if user
        else URL(account=account, database=database, role=role)
    )

    with patch(
        "dags.talkdesk_import.BaseHook.get_connection", return_value=mock
    ) as mock_conn, patch(
        "dags.talkdesk_import.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            url, connect_args={"authenticator": "externalbrowser",},
        ),
    ) as mock_engine:
        import_talkdesk(
            "snowflake_conn",
            "talkdesk_conn",
            schema="TALKDESK",
            bucket_name="tc-talkdesk",
            execution_date=pendulum.datetime(2018, 2, 22),
            next_execution_date=pendulum.datetime(2018, 2, 23),
            cmk_key_id="04bc297e-6ec2-4fa0-b3aa-ffb29d40f306",
        )
else:
    globals()["talkdeskdag"] = create_dag()
