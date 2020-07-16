import datetime
import json
import os
from time import sleep
from typing import Any

import pendulum
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from authlib.integrations.requests_client import OAuth2Session
from fs import open_fs
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

MAX_RUNS = 360


def import_talkdesk(
    snowflake_conn: str,
    talkdesk_conn: str,
    schema: str,
    bucket_name: str,
    execution_date: pendulum.datetime,
    next_execution_date: pendulum.datetime,
    **kwargs: Any,
) -> None:
    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    taskdesk_connection = BaseHook.get_connection(talkdesk_conn)

    engine: Engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()

    session = OAuth2Session(
        client_id=taskdesk_connection.login,
        client_secret=taskdesk_connection.password,
        scope="reports:read reports:write recordings:read",
        token_endpoint=taskdesk_connection.host,
        grant_type="client_credentials",
    )
    session.fetch_token()

    initial_payload = {
        "format": "json",
        "timespan": {
            "from": execution_date.isoformat(),
            "to": next_execution_date.isoformat(),
        },
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
        print(f"💤 Sleeping for 10 seconds because status is {status} ({run})")
        sleep(10)
        resp = session.get(
            data["_links"]["self"]["href"]
        )  # Will be autoredirected to the report file

        run += 1

    if status == "failed":
        raise Exception(resp.text)

    for entry in data.get("entries", []):
        recordings = (
            session.get(recording_url).json()
            if (recording_url := entry.get("recording_url"))
            else None
        )

        with engine.begin() as tx:
            tx.execute(
                f"insert into {schema}.CALLS "
                f"select parse_json('{json.dumps(entry)}') as CALL, "
                f"parse_json('{json.dumps(recordings)}') as RECORDINGS"  # nosec
            )

        print(f"call id: {entry.get('call_id')} recording: {recordings}")

        if not recordings:
            print(f"⚠️Could not find recording_url key in {entry.get('call_id')}")
            continue

        if not (embedded := recordings.get("_embedded")):
            print(f"⚠️Could not find _embedded key in {entry.get('call_id')}")
            continue

        for recording in embedded.get("recordings", []):
            media_link = recording["_links"]["media"]["href"]
            with session.get(media_link, stream=True) as in_, open_fs(
                f"s3://{bucket_name}", create=True
            ) as fs, fs.open(f"{recording['id']}.mp3", mode="w+b") as out_:
                for chunk in in_.iter_content(chunk_size=8192):
                    out_.write(chunk)


def create_dag() -> DAG:
    with DAG(
        "talkdesk_import",
        start_date=pendulum.datetime(
            2015, 1, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=None,
        catchup=True,
    ) as dag:
        dag << PythonOperator(
            task_id="import_talkdesk",
            python_callable=import_talkdesk,
            op_kwargs={
                "snowflake_conn": "snowflake_talkdesk",
                "talkdesk_conn": "http_talkdesk",
                "schema": "TALKDESK",
                "bucket_name": "tc-talkdesk"
            },
            provide_context=True,
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config={
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowProductionTalkdeskRole"
                },
                "resources": {"request_memory": "2Gi"},
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
            task_instance_key_str="manual_testing",
            execution_date=pendulum.datetime(2019, 9, 29),
            next_execution_date=pendulum.datetime(2019, 9, 30),
        )
else:
    globals()["talkdeskdag"] = create_dag()
