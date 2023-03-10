from datetime import datetime, timedelta
from typing import Any

import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from numpy import datetime64
from sqlalchemy import text, cast, column, Date
from sqlalchemy.sql import Select
from zeep import Client

from utils.failure_callbacks import slack_task


def build_journal_entry(
    correlation_guid: str,
    grouped_transactions: pd.DataFrame,
    client: Client,
    created_at: datetime,
) -> Any:
    # get subsidiary
    recordref_type = client.get_type("ns0:RecordRef")
    subsidiary = recordref_type(
        internalId=grouped_transactions["ns_subsidiary_id"].iloc[0], type="subsidiary"
    )
    # get transaction_date
    datetime_type = client.get_type("xsd:dateTime")
    transaction_date = datetime_type(
        pendulum.instance(grouped_transactions["posted_at"].max(), tz="America/Toronto")
    )

    # build journal_entry_line_list
    journalentryline_type = client.get_type("ns31:JournalEntryLine")
    line_list = []
    for values in (
        grouped_transactions[
            ["credit_amount", "debit_amount", "ns_account_internal_id"]
        ]
        .fillna(0)
        .values
    ):
        if values[0]:
            line_list.append(
                journalentryline_type(
                    account=recordref_type(internalId=int(values[2]), type="account"),
                    credit=values[0],
                    memo=f"Transaction ID: {correlation_guid}",
                )
            )
        if values[1]:
            line_list.append(
                journalentryline_type(
                    account=recordref_type(internalId=int(values[2]), type="account"),
                    debit=values[1],
                    memo=f"Transaction ID: {correlation_guid}",
                )
            )
    journalentrylinelist_type = client.get_type("ns31:JournalEntryLineList")

    # get journal_entry type
    journalentry_type = client.get_type("ns31:JournalEntry")
    return journalentry_type(
        externalId=correlation_guid,
        subsidiary=subsidiary,
        lineList=journalentrylinelist_type(line=line_list),
        tranDate=transaction_date,
        memo=f"Platform Transaction - {created_at}",
    )


def process_grouped_transactions(
    correlation_guid: str,
    grouped_transactions: pd.DataFrame,
    client: Client,
    passport: str,
    app_info: str,
    created_at: datetime,
) -> str:
    # check subsidiary: allow only one subsidiary value
    if grouped_transactions["ns_subsidiary_id"].nunique() != 1:
        raise ValueError("Different subsidiary")

    # call Add(journal_entry)
    re = client.service.add(
        build_journal_entry(correlation_guid, grouped_transactions, client, created_at),
        _soapheaders={"passport": passport, "applicationInfo": app_info},
    )

    # check response
    status = re.body.writeResponse.status
    if status.isSuccess and re.body.writeResponse.baseRef.internalId:
        return re.body.writeResponse.baseRef.internalId
    else:
        raise ValueError(status)


def print_endpoint(client: Client) -> None:
    definitions = client.wsdl._definitions
    definition = definitions[list(definitions.keys())[0]]
    endpoint = (
        definition.services["NetSuiteService"]
        .ports["NetSuitePort"]
        .binding_options["address"]
    )
    print(f"Endpoint: {endpoint}")


def create_journal_entry_for_transaction(ds: datetime, **kwargs: Any) -> None:
    snowflake_hook = BaseHook.get_connection("snowflake_platform_erp")
    snowflake_vars = {
        "src_schema": snowflake_hook.extra_dejson.get("src_schema"),
        "src_database": snowflake_hook.extra_dejson.get("src_database"),
    }

    netsuite_hook = BaseHook.get_connection("netsuite")
    netsuite_vars = {
        "email": netsuite_hook.login,
        "password": netsuite_hook.password,
        "account": netsuite_hook.schema,
        "app_id": netsuite_hook.extra_dejson.get("app_id"),
        "endpoint": netsuite_hook.host,
        "wsdl": netsuite_hook.extra_dejson.get("wsdl"),
    }

    created_date = ds
    print(f"Created_date: {created_date}")

    selectable = Select(
        [text("*")],
        from_obj=text(
            f'"{snowflake_vars["src_database"]}".{snowflake_vars["src_schema"]}.fct_platform_erp_transactions'
        ),
    ).where(cast(column("created_at"), Date) == text(f"'{created_date}'"))

    with SnowflakeHook("snowflake_production").get_sqlalchemy_engine().begin() as tx:
        df = pd.read_sql(
            selectable,
            tx,
            columns=(
                "correlation_guid",
                "posted_at",
                "credit_amount",
                "debit_amount",
                "account_number",
                "facility",
                "ns_account_internal_id",
                "ns_subsidiary_id",
            ),
        )
        df.astype(
            {
                "correlation_guid": object,
                "posted_at": datetime64,
                "credit_amount": object,
                "debit_amount": object,
                "account_number": object,
                "facility": object,
                "ns_account_internal_id": object,
                "ns_subsidiary_id": object,
            }
        )
        groups = df.groupby("correlation_guid")
        succeeded = []
        failed = []
        # login SOAP client
        client = Client(netsuite_vars["wsdl"])

        passport_type = client.get_type("ns0:Passport")
        passport = passport_type(
            email=netsuite_vars["email"],
            password=netsuite_vars["password"],
            account=netsuite_vars["account"],
        )

        applicationinfo_type = client.get_type("ns4:ApplicationInfo")
        app_info = applicationinfo_type(applicationId=netsuite_vars["app_id"])

        client.service.login(
            passport=passport, _soapheaders={"applicationInfo": app_info}
        )

        print_endpoint(client)
        data_center_urls = client.service.getDataCenterUrls(netsuite_vars["account"])
        print(
            f"Use DataCenterUrl: {data_center_urls.body.getDataCenterUrlsResult.dataCenterUrls.webservicesDomain}"
        )

        for group in groups:
            correlation_guid = group[0]
            try:
                journal_entry_internal_id = process_grouped_transactions(
                    correlation_guid, group[1], client, passport, app_info, created_date
                )
                print(
                    f"Journal Entry uploaded: {correlation_guid} - {journal_entry_internal_id}"
                )
                succeeded.append(
                    {
                        "uploaded_at": datetime.utcnow(),
                        "ns_journal_entry_internal_id": journal_entry_internal_id,
                        "correlation_guid": correlation_guid,
                    }
                )
            except ValueError as e:
                print(f"Error: Journal Entry failed: {correlation_guid} - {e}")
                failed.append(
                    {
                        "failed_at": datetime.utcnow(),
                        "error": str(e),
                        "correlation_guid": correlation_guid,
                    }
                )
        print(f"Total succeeded: {len(succeeded)} - Total failed: {len(failed)}")


with DAG(
    "platform_journal_entry",
    max_active_runs=1,
    catchup=True,
    schedule_interval="@daily",
    start_date=pendulum.datetime(
        2020, 8, 29, tzinfo=pendulum.timezone("America/Toronto")
    ),
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=30),
        "on_failure_callback": slack_task("slack_data_alerts"),
    },
    description="Platform ERP Pipeline",
) as dag:
    dag << PythonOperator(
        task_id="get_transactions_by_created_date",
        python_callable=create_journal_entry_for_transaction,
        provide_context=True,
        pool="netsuite_pool",
        dag=dag,
    )
