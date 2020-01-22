import json
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from jinja2 import Template
import pandas as pd
import requests
import snowflake.connector
import xmltodict
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from netsuite_extras import netsuite_helper

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 22),
    "retries": 0,
}

aws_hook = AwsHook(aws_conn_id="s3_conn_id")
aws_credentials = aws_hook.get_credentials()

fun = "Exception report"
env = "test"
roles = ["analyst_role", "looker_role"]
with DAG(
    "journal_entry", schedule_interval="0 13 * * *", default_args=default_args
) as dag:
    snowflake_hook = BaseHook.get_connection("snowflake_erp")
    netsuite_hook = BaseHook.get_connection("netsuite")
    netsuite = {
        "email": netsuite_hook.login,
        "password": netsuite_hook.password,
        "account": netsuite_hook.schema,
        "app_id": json.loads(netsuite_hook.extra)["app_id"],
        "endpoint": netsuite_hook.host,
        "recipients": json.loads(netsuite_hook.extra)["recipients"],
    }

    def get_tli_by_created_date(**kwargs):
        cd = kwargs["ds"]
        date_tag = cd.replace("-", "")
        print(f"Created_date: {kwargs['ds']}")
        with open(
            "airflow/dags/netsuite_extras/queries/get_tli_by_created_date.sql"
        ) as f:
            sql_template = Template(f.read())
        sql = sql_template.render(
            created_date=cd, env=f"{env}", create_date_trim=date_tag
        )

        count_all = f"select count(*) from erp.{env}.tli_{date_tag};"
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            for role in roles:
                cur.execute(f"grant select on erp.{env}.tli_{date_tag} to role {role};")
            cur.execute(count_all)
            if cur.fetchone()[0] == 0:
                return "task_end"
            return "task_get_unbalanced_tli_by_subsidiary"

    def get_unbalanced_tli_by_subsidiary(**kwargs):
        date_tag = kwargs["ds"].replace("-", "")
        with open(
            "airflow/dags/netsuite_extras/queries/get_unbalanced_tran_by_subsidiary.sql"
        ) as f:
            sql_template = Template(f.read())
            sql = sql_template.render(env="test", created_date_trim=date_tag)
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            for role in roles:
                cur.execute(
                    f"grant select on erp.{env}.tli_unbalanced_{date_tag} to role {role};"
                )

    def send_email_for_unbalanced_tli(**kwargs):
        date_tag = kwargs["ds"].replace("-", "")
        print("send_email_for_unbalanced_tli")
        sql = f"select * from erp.{env}.tli_unbalanced_{date_tag}"
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data)
                df.columns = [
                    "id",
                    "tran_id",
                    "new_gl",
                    "old_gl",
                    "account_internal_id",
                    "subsidiary",
                    "subsidiary_id",
                    "document_description",
                    "credit",
                    "debit",
                    "tran_date",
                    "created_date",
                ]
                subject = f"Unbalanced transaction line items created on {date_tag}"
                file_name = f"/tmp/tli_unbalanced_{date_tag}.csv"
                df.to_csv(file_name, index=False, sep="\t")
                with open(file_name, "r") as f:
                    print("Send email: unbalanced tli")
                    send_email(
                        to=netsuite["recipients"],
                        # to=['xzhang@thinkingcapital.ca'],
                        subject=subject,
                        html_content=fun,
                        files=[file_name],
                    )
                return "Email sent"
            return "Nothing to send"

    def update_tli_unbalanced_historical(**kwargs):
        date_tag = kwargs["ds"].replace("-", "")
        execution_time = kwargs["ts"]
        with open(
            "airflow/dags/netsuite_extras/queries/update_tli_unbalanced_historical.sql"
        ) as f:
            sql_template = Template(f.read())
            sql = sql_template.render(
                execition_time=execution_time, created_date_trim=date_tag, env=env
            )
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            return "Update to historical table"

    def send_journal_entries(**kwargs):
        created_date = kwargs["ds"]
        with open(
            "airflow/dags/netsuite_extras/queries/create_journal_entry_line.sql"
        ) as f:
            sql_template = Template(f.read())
            sql = sql_template.render(
                created_date=created_date,
                env="test",
                create_date_trim=created_date.replace("-", ""),
            )
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data)
                df.columns = [
                    "tran_date",
                    "old_gl",
                    "account_internal_id",
                    "subsidiary_id",
                    "credit",
                    "debit",
                ]
                data_json = json.loads(df.to_json(orient="records"))
                # group gl/transaction_date
                rows = {}
                for item in data_json:
                    key = (
                        f"{item['tran_date'].replace('-', '')}_{item['subsidiary_id']}"
                    )
                    if key not in rows:
                        rows[key] = []
                    rows[key].append(item)

                for key in rows:
                    print(f"{key}:{rows[key]}")
                    upload_journal_entry(created_date, rows[key])
            else:
                print("Nothing to send")

    def upload_journal_entry(created_date, rows):
        # print(rows)
        execution_time = datetime.utcnow()
        try:
            tran_date = rows[0]["tran_date"]
            subsidiary_id = rows[0]["subsidiary_id"]
            payload = netsuite_helper.get_journal_entry_payload(
                created_date,
                rows,
                netsuite["email"],
                netsuite["password"],
                netsuite["account"],
                netsuite["app_id"],
            )
            credit = 0
            debit = 0
            for row in rows:
                if row["credit"]:
                    credit += row["credit"]
                if row["debit"]:
                    debit += row["debit"]
            print(f"{tran_date}:sub[{subsidiary_id}] - c:{credit} | d:{debit}")
            # print(f"payload: {payload}")
            headers = {
                "content-type": "application/xml",
                "soapaction": "Add",
                "cache-control": "no-cache",
            }
            r = requests.post(netsuite["endpoint"], headers=headers, data=payload)
            j = xmltodict.parse(r.content)
            write_response = j["soapenv:Envelope"]["soapenv:Body"]["addResponse"][
                "writeResponse"
            ]
            if write_response["platformCore:status"]["@isSuccess"] == "true":
                je_internal_id = write_response["baseRef"]["@internalId"]
                log_status(
                    je_internal_id,
                    "TRUE",
                    "",
                    execution_time,
                    created_date,
                    tran_date,
                    subsidiary_id,
                )
            else:
                code = write_response["platformCore:status"][
                    "platformCore:statusDetail"
                ]["platformCore:code"]
                message = write_response["platformCore:status"][
                    "platformCore:statusDetail"
                ]["platformCore:message"]
                error_message = code + ":" + message
                log_status(
                    "",
                    "FALSE",
                    error_message,
                    execution_time,
                    created_date,
                    tran_date,
                    subsidiary_id,
                )
                print(f"Payload: {rows}")
        except Exception as e:
            raise e

    def log_uploaded(**kwargs):
        print("log_uploaded")
        created_date = kwargs["ds"]
        with open("airflow/dags/netsuite_extras/queries/log_uploaded.sql") as f:
            sql_template = Template(f.read())
        log_history_sql = sql_template.render(
            env="test", created_date_trim=created_date.replace("-", "")
        )
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(log_history_sql)

    def log_error(**kwargs):
        print("log_uploaded")
        created_date = kwargs["ds"]
        with open("airflow/dags/netsuite_extras/queries/log_error.sql") as f:
            sql_template = Template(f.read())
        log_history_sql = sql_template.render(
            env="test", created_date_trim=created_date.replace("-", "")
        )
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(log_history_sql)

    def log_status(
        je_internal_id,
        uploaded,
        error_msg,
        execution_time,
        created_date,
        transaction_date,
        subsidiary_id,
    ):
        print(f"log_status:{uploaded}")
        with open("airflow/dags/netsuite_extras/queries/log_status.sql") as f:
            sql_template = Template(f.read())
        log_status_sql = sql_template.render(
            je_internal_id=je_internal_id,
            subsidiary_id=subsidiary_id,
            uploaded=uploaded,
            error_msg=error_msg,
            created_date=created_date,
            transaction_date=transaction_date,
            execution_time=execution_time,
            env="test",
        )
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse="ETL",
            database="ERP",
            schema="PUBLIC",
            ocsp_fail_open=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(log_status_sql)

    task_filter_tli_on_created_date = BranchPythonOperator(
        task_id="task_filter_tli_on_created_date",
        python_callable=get_tli_by_created_date,
        provide_context=True,
        dag=dag,
    )

    task_get_unbalanced_tli_by_subsidiary = PythonOperator(
        task_id="task_get_unbalanced_tli_by_subsidiary",
        python_callable=get_unbalanced_tli_by_subsidiary,
        provide_context=True,
        dag=dag,
    )

    task_send_email_for_unbalanced_tli = PythonOperator(
        task_id="task_send_email_for_unbalanced_tli",
        python_callable=send_email_for_unbalanced_tli,
        provide_context=True,
        dag=dag,
    )

    task_create_journal_entry = PythonOperator(
        task_id="task_create_journal_entry",
        python_callable=send_journal_entries,
        provide_context=True,
        pool="netsuite_pool",
        dag=dag,
    )

    task_end = DummyOperator(task_id="task_end", dag=dag)

    task_log_uploaded = PythonOperator(
        task_id="task_log_uploaded",
        python_callable=log_uploaded,
        provide_context=True,
        dag=dag,
    )

    task_log_error = PythonOperator(
        task_id="task_log_error",
        python_callable=log_error,
        provide_context=True,
        dag=dag,
    )

    task_update_tli_unbalanced_historical = PythonOperator(
        task_id="task_update_tli_unbalanced_historical",
        python_callable=update_tli_unbalanced_historical,
        provide_context=True,
        dag=dag,
    )

    task_filter_tli_on_created_date >> [task_get_unbalanced_tli_by_subsidiary, task_end]
    task_get_unbalanced_tli_by_subsidiary >> [
        task_send_email_for_unbalanced_tli,
        task_create_journal_entry,
    ]
    task_create_journal_entry >> [task_log_uploaded, task_log_error] >> task_end
    task_send_email_for_unbalanced_tli >> task_update_tli_unbalanced_historical
