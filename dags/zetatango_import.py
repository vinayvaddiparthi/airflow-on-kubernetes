import logging
import os
import random
import string
import tempfile
from pathlib import Path
from typing import List

import attr
import pandas as pd
import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import subprocess  # nosec

from sqlalchemy import text, func
from sqlalchemy.sql import Select


@attr.s
class ColumnSpec:
    schema: str = attr.ib()
    table: str = attr.ib()
    columns: List[str] = attr.ib()
    catalog: str = attr.ib(default=None)


def _bash_command(app, snowflake_connection, snowflake_schema):
    snowflake_conn = SnowflakeHook.get_connection(snowflake_connection)
    env = ";".join(
        [
            f"{k}={v}"
            for k, v in {
                "SNOWFLAKE_ACCOUNT": "thinkingcapital.ca-central-1.aws",
                "SNOWFLAKE_USERNAME": snowflake_conn.login,
                "SNOWFLAKE_PASSWORD": snowflake_conn.password,
                "SNOWFLAKE_DATABASE": "ZETATANGO",
                "SNOWFLAKE_SCHEMA": snowflake_schema,
            }.items()
        ]
    )

    return f"""
    mkdir --parents --mode=700 ~/.ssh &&\
    echo $SSH_PRIVATE_KEY > ~/.ssh/id_rsa &&\
    chmod 600 ~/.ssh/id_rsa &&\
    /usr/local/bin/heroku run --app={app} --env={env} --exit-code python extract.py
    """


def decrypt_pii_columns(
    snowflake_connection: str, column_specs: List[ColumnSpec], target_schema: str
):
    from pyporky.symmetric import SymmetricPorky
    from json import loads as json_loads
    from base64 import b64decode

    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    def __decrypt(row):
        list_ = [row[0]]
        for field in row[1:]:
            crypto_material = json_loads(field)
            list_.append(
                SymmetricPorky(aws_region="ca-central-1").decrypt(
                    enciphered_dek=b64decode(crypto_material["key"]),
                    enciphered_data=b64decode(crypto_material["data"]),
                    nonce=b64decode(crypto_material["nonce"]),
                )
            )

        return list_

    for cs in column_specs:
        stmt = Select(
            columns=[text(f"{cs.table}.$1:id AS id")]
            + [
                func.base64_decode_string(text(f"{cs.table}.$1:encrypted_{col}")).label(
                    col
                )
                for col in cs.columns
            ],
            from_obj=text(f"{cs.schema}.{cs.table}"),
        )

        engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        with engine.begin() as tx, tempfile.TemporaryDirectory() as output_directory:
            path = Path(output_directory) / f"{cs.table}.parquet"

            df = pd.read_sql(stmt, con=tx).apply(
                axis=1, func=__decrypt, result_type="broadcast"
            )
            df.to_parquet(path, engine="fastparquet", compression="gzip")

            stage = "".join(
                random.choice(string.ascii_uppercase) for _ in range(24)  # nosec
            )

            logging.info(
                [
                    tx.execute(stmt).fetchall()
                    for stmt in [
                        f"CREATE OR REPLACE TEMPORARY STAGE {target_schema}.{stage} FILE_FORMAT=(TYPE=PARQUET)",
                        f"PUT file://{path} @{target_schema}.{stage}",
                        f"CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{cs.schema}__{cs.table} AS SELECT * FROM @{target_schema}.{stage}",
                    ]
                ]
            )


with DAG(
    dag_id="zetatango_import",
    start_date=pendulum.datetime(
        2020, 4, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="0 0,8-20 * * 1-5",
) as dag:
    dag << BashOperator(
        task_id="zt-production-elt-core__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_production_elt_core",
            snowflake_connection="snowflake_zetatango_production",
            snowflake_schema="CORE_PRODUCTION",
        ),
    ) >> PythonOperator(
        task_id="zt-production-elt-core__pii_decryption",
        python_callable=decrypt_pii_columns,
        op_kwargs={
            "snowflake_connection": "snowflake_zetatango_production",
            "column_specs": [
                ColumnSpec(
                    schema="CORE_PRODUCTION",
                    table="MERCHANT_ATTRIBUTES",
                    columns=["value"],
                )
            ],
            "target_schema": "PII_PRODUCTION",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/KubernetesAirflowProductionZetatangoPiiRole"
                }
            }
        },
    )

    dag << BashOperator(
        task_id="zt-production-elt-idp__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_production_elt_idp",
            snowflake_connection="snowflake_zetatango_production",
            snowflake_schema="IDP_PRODUCTION",
        ),
    )

    dag << BashOperator(
        task_id="zt-production-elt-kyc__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_production_elt_kyc",
            snowflake_connection="snowflake_zetatango_production",
            snowflake_schema="KYC_PRODUCTION",
        ),
    )

    dag << BashOperator(
        task_id="zt-staging-elt-core__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_staging_elt_core",
            snowflake_connection="snowflake_zetatango_staging",
            snowflake_schema="CORE_STAGING",
        ),
    ) >> PythonOperator(
        task_id="zt-staging-elt-core__pii_decryption",
        python_callable=decrypt_pii_columns,
        op_kwargs={
            "snowflake_connection": "snowflake_zetatango_staging",
            "column_specs": [
                ColumnSpec(
                    schema="CORE_STAGING",
                    table="MERCHANT_ATTRIBUTES",
                    columns=["value"],
                )
            ],
            "target_schema": "PII_STAGING",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/KubernetesAirflowNonProdZetatangoPiiRole"
                }
            }
        },
    )

    dag << BashOperator(
        task_id="zt-staging-elt-idp__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_staging_elt_idp",
            snowflake_connection="snowflake_zetatango_staging",
            snowflake_schema="IDP_STAGING",
        ),
    )

    BashOperator(
        task_id="zt-staging-elt-kyc__import",
        env={
            "HOME": os.environ["HOME"],
            "SSH_PRIVATE_KEY": SSHHook.get_connection(
                "heroku_production_ssh_key"
            ).extra_dejson["private_key"],
            "HEROKU_API_KEY": HttpHook.get_connection(
                "heroku_production_api_key"
            ).password,
        },
        bash_command=_bash_command(
            app="zt_staging_elt_kyc",
            snowflake_connection="snowflake_zetatango_staging",
            snowflake_schema="KYC_STAGING",
        ),
    )
