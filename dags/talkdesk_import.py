import base64
import datetime
import json
import logging
import os
from io import BytesIO
from time import sleep
from typing import Any, Optional, Dict

import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from authlib.integrations.requests_client import OAuth2Session
from fs_s3fs import S3FS
from sqlalchemy import create_engine, func, Table, MetaData, Column
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select
from snowflake.sqlalchemy import VARIANT

from pyporky.symmetric import SymmetricPorky

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

    session = OAuth2Session(
        client_id=taskdesk_connection.login,
        client_secret=taskdesk_connection.password,
        scope="reports:read reports:write recordings:read",
        token_endpoint=taskdesk_connection.host,
        grant_type="client_credentials",
    )
    session.fetch_token()

    for i in range((next_execution_date - execution_date).days):
        start_dt = execution_date + datetime.timedelta(days=i)
        end_dt = start_dt + datetime.timedelta(days=1)

        logging.info(f"⚙ Importing calls from {start_dt} to {end_dt}")

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
        porky = SymmetricPorky(aws_region="ca-central-1")

        entries = data.get("entries", [])
        logging.info(f"entries list contains {len(entries)} records")

        for entry in entries:  # for each entry in the calls report
            call_id: str = entry["call_id"]

            # get the recordings report associated with the entry
            recordings: Optional[Dict] = (
                session.get(recording_url).json()
                if (recording_url := entry.get("recording_url"))
                else None
            )

            logging.debug(f"call id: {call_id} recording: {recordings}")

            #  insert call and recordings metadata in Snowflake
            with engine.begin() as tx:
                selectable = Select(
                    [
                        func.parse_json(json.dumps(entry)).label("call"),
                        func.parse_json(json.dumps(recordings)).label("recordings"),
                    ]
                )
                tx.execute(t.insert().from_select(["call", "recordings"], selectable))

            if not recordings:
                logging.warning(f"⚠️Could not find recording_url key in {call_id}")
                continue

            if not (embedded := recordings.get("_embedded")):
                logging.warning(f"⚠️Could not find _embedded key in {call_id}")
                continue

            # download all recordings and re-upload them to S3
            for recording in embedded.get("recordings", []):
                media_link = recording["_links"]["media"]["href"]
                with session.get(
                    media_link, stream=True
                ) as in_, BytesIO() as intrm_, fs.open(
                    f"{recording['id']}.mp3", mode="w+b"
                ) as out_:
                    for chunk in in_.iter_content(chunk_size=8192):
                        intrm_.write(chunk)
                        key, data, nonce = porky.encrypt(
                            data=intrm_.read(), cmk_key_id=cmk_key_id,
                        )
                    out_.write(
                        json.dumps(
                            {
                                "key": base64.b64encode(key).decode("utf-8"),
                                "data": base64.b64encode(data).decode("utf-8"),
                                "nonce": base64.b64encode(nonce).decode("utf-8"),
                            }
                        ).encode("utf-8")
                    )

        logging.info(f"✨ Done importing calls from {start_dt} to {end_dt}")


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
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
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
            execution_date=pendulum.datetime(2018, 1, 1),
            next_execution_date=pendulum.datetime(2018, 6, 15),
            cmk_key_id="04bc297e-6ec2-4fa0-b3aa-ffb29d40f306",
        )
else:
    globals()["talkdeskdag"] = create_dag()
