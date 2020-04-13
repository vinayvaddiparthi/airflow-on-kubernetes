import json
import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template
import pandas as pd
import snowflake.connector
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from zeep import Client

snowflake_hook = BaseHook.get_connection("snowflake_platform_erp")
snowflake_vars = {
    "src_schema": json.loads(snowflake_hook.extra)["src_schema"],
    "src_database": json.loads(snowflake_hook.extra)["src_database"],
    "dest_schema": json.loads(snowflake_hook.extra)["dest_schema"],
    "dest_database": json.loads(snowflake_hook.extra)["dest_database"],
    "warehouse": json.loads(snowflake_hook.extra)["warehouse"],
}

netsuite_hook = BaseHook.get_connection("netsuite")
netsuite_vars = {
    "email": netsuite_hook.login,
    "password": netsuite_hook.password,
    "account": netsuite_hook.schema,
    "app_id": json.loads(netsuite_hook.extra)["app_id"],
    "endpoint": netsuite_hook.host,
    "wsdl": json.loads(netsuite_hook.extra)["wsdl"],
}


def process_grouped_transactions(
    correlation_guid, grouped_transactions, client, passport, app_info, created_at
) -> str:
    # check subsidiary: allow only one subsidiary value
    if grouped_transactions["ns_subsidiary_id"].nunique() != 1:
        raise ValueError("Different subsidiary")

    # get subsidiary
    record_ref = client.get_type("ns0:RecordRef")
    subsidiary = record_ref(
        internalId=grouped_transactions["ns_subsidiary_id"].max(), type="subsidiary"
    )
    # get tranDate
    dateTime = client.get_type("xsd:dateTime")
    tran_date = dateTime(grouped_transactions["posted_at"].max())

    # build journal_entry_line_list
    journal_entry_line = client.get_type("ns31:JournalEntryLine")
    line_list = []
    for values in grouped_transactions[
        ["credit_amount", "debit_amount", "ns_account_internal_id"]
    ].values:
        if values[0]:
            line_list.append(
                journal_entry_line(
                    account=record_ref(internalId=values[2], type="account"),
                    credit=values[0],
                    memo=f"Platform Transaction - {correlation_guid} - {created_at}",
                )
            )
        if values[1]:
            line_list.append(
                journal_entry_line(
                    account=record_ref(internalId=values[2], type="account"),
                    debit=values[1],
                    memo=f"Platform Transaction - {correlation_guid} - {created_at}",
                )
            )
    journal_entry_line_list = client.get_type("ns31:JournalEntryLineList")

    # get journal_entry type
    journal_entry = client.get_type("ns31:JournalEntry")

    # call Add(journal_entry)
    re = client.service.add(
        journal_entry(
            subsidiary=subsidiary,
            lineList=journal_entry_line_list(line=line_list),
            tranDate=tran_date,
            memo=f"Platform Transaction - {correlation_guid} - {created_at}",
        ),
        _soapheaders={"passport": passport, "applicationInfo": app_info},
    )

    # check response
    status = re.body.writeResponse.status
    if status.isSuccess and re.body.writeResponse.baseRef.internalId:
        return re.body.writeResponse.baseRef.internalId
    else:
        raise ValueError(f"{status.statusDetail.message}")


def print_endpoint(client):
    definitions = client.wsdl._definitions
    definition = definitions[list(definitions.keys())[0]]
    endpoint = (
        definition.services["NetSuiteService"]
        .ports["NetSuitePort"]
        .binding_options["address"]
    )
    print(f"Endpoint: {endpoint}")


def create_journal_entry_for_transaction(**context):
    created_date = context["ds"]
    print(f"Created_date: {context['ds']}")
    # find path of template file
    with open("netsuite_extras/queries_for_platform/get_daily_transactions.sql") as f:
        sql_template = Template(f.read())
    sql = sql_template.render(
        created_at=created_date,
        db_name=snowflake_vars["src_database"],
        schema_name=snowflake_vars["src_schema"],
    )
    with SnowflakeHook(
        "snowflake_platform_erp"
    ).get_sqlalchemy_engine().begin() as conn:
        df = pd.read_sql(
            sql,
            conn,
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

        app_info_type = client.get_type("ns4:ApplicationInfo")
        app_info = app_info_type(applicationId=netsuite_vars["app_id"])

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
                je_iid = process_grouped_transactions(
                    correlation_guid, group[1], client, passport, app_info, created_date
                )
                print(f"Journal Entry uploaded: {correlation_guid} - {je_iid}")
                succeeded.append(("placeholder", je_iid, correlation_guid))
            except ValueError as e:
                print(f"Error: Journal Entry failed: {correlation_guid} - {e}")
                failed.append(("placeholder", e, correlation_guid))
        with snowflake.connector.connect(
            user=snowflake_hook.login,
            password=snowflake_hook.password,
            account=snowflake_hook.host,
            warehouse=snowflake_vars["warehouse"],
            database=snowflake_vars["dest_database"],
            schema=snowflake_vars["dest_schema"],
            ocsp_fail_open=False,
        ) as snow_conn:
            cur = snow_conn.cursor()
            if succeeded:
                succeeded_values = str(succeeded)[1:-1].replace(
                    "'placeholder'", "current_timestamp::timestamp_ntz"
                )
                log_upload = (
                    f"insert into erp.platform.uploaded values {succeeded_values}"
                )
                cur.execute(log_upload)
                print(f"succeeded: {len(succeeded)}")
            if failed:
                failed_values = str(failed)[1:-1].replace(
                    "'placeholder'", "current_timestamp::timestamp_ntz"
                )
                log_failed = f"insert into erp.platform.failed values {failed_values}"
                cur.execute(log_failed)
                print(f"failed: {len(failed)}")
            cur.commit()


with DAG(
    "platform_journal_entry",
    max_active_runs=1,
    schedule_interval="@daily",
    start_date=pendulum.datetime(
        2020, 4, 10, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << PythonOperator(
        task_id="get_transactions_by_created_date",
        python_callable=create_journal_entry_for_transaction,
        provide_context=True,
        pool="netsuite_pool",
        dag=dag,
    )
