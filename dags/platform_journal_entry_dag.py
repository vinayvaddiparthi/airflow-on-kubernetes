import json
import logging
import pathlib
import pendulum
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template
import pandas as pd
import snowflake.connector
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from zeep import Client

logger = logging.getLogger("platform_journal_entry")
logger.setLevel(logging.DEBUG)

snowflake_hook = BaseHook.get_connection("snowflake_erp")
netsuite_hook = BaseHook.get_connection("netsuite")
netsuite = {
    "email": netsuite_hook.login,
    "password": netsuite_hook.password,
    "account": netsuite_hook.schema,
    "app_id": json.loads(netsuite_hook.extra)["app_id"],
    "endpoint": netsuite_hook.host,
    "wsdl": json.loads(netsuite_hook.extra)["wsdl"],
}


def process_grouped_transactions(correlation_guid, df, client, ds):
    # check subsidiary
    if df["ns_subsidiary_id"].max() != df["ns_subsidiary_id"].min():
        raise ValueError("Different subsidiary")

    try:

        # get subsidiary
        record_ref = client.get_type("ns0:RecordRef")
        subsidiary = record_ref(
            internalId=df["ns_subsidiary_id"].max(), type="subsidiary"
        )
        # get tranDate
        dateTime = client.get_type("xsd:dateTime")
        tran_date = dateTime(df["posted_at"].max())

        # build journal_entry_line_list
        journal_entry_line = client.get_type("ns31:JournalEntryLine")
        line_list = []
        for values in df[
            ["credit_amount", "debit_amount", "ns_account_internal_id"]
        ].values:
            if values[0] is not None:
                line_list.append(
                    journal_entry_line(
                        account=record_ref(internalId=values[2], type="account"),
                        credit=values[0],
                        memo=f"Platform Transaction - {correlation_guid} - {ds}",
                    )
                )
            if values[1] is not None:
                line_list.append(
                    journal_entry_line(
                        account=record_ref(internalId=values[2], type="account"),
                        debit=values[1],
                        memo=f"Platform Transaction - {correlation_guid} - {ds}",
                    )
                )
        journal_entry_line_list = client.get_type("ns31:JournalEntryLineList")
        journal_entry_line_list = journal_entry_line_list(line=line_list)

        # build journal_entry
        journal_entry = client.get_type("ns31:JournalEntry")
        je = journal_entry(
            subsidiary=subsidiary,
            lineList=journal_entry_line_list,
            tranDate=tran_date,
            memo=f"Platform Transaction - {correlation_guid} - {ds}",
        )

        # call Add(journal_entry)
        re = client.service.add(
            je,
            _soapheaders={
                "passport": make_passport(client),
                "applicationInfo": get_app_info(client),
            },
        )

        # check response
        status = re.body.writeResponse.status
        succeed = status.isSuccess
        if succeed and re.body.writeResponse.baseRef.internalId:
            return re.body.writeResponse.baseRef.internalId
        else:
            raise ValueError(f"{status.statusDetail.message}")
    except Exception as e:
        raise e


def get_endpoint(client):
    definitions = client.wsdl._definitions
    definition = definitions[list(definitions.keys())[0]]
    endpoint = (
        definition.services["NetSuiteService"]
        .ports["NetSuitePort"]
        .binding_options["address"]
    )
    return endpoint


def get_app_info(client):
    app_info = client.get_type("ns4:ApplicationInfo")
    info = app_info(applicationId=netsuite["app_id"])
    logger.debug(info)
    return info


def make_passport(client):
    passport = client.get_type("ns0:Passport")
    return passport(
        email=netsuite["email"],
        password=netsuite["password"],
        account=netsuite["account"],
    )


def login_client():
    client = Client(netsuite["wsdl"])
    client.service.login(
        passport=make_passport(client),
        _soapheaders={"applicationInfo": get_app_info(client)},
    )
    return client


with DAG(
    "platform_journal_entry",
    max_active_runs=1,
    schedule_interval="@daily",
    start_date=pendulum.datetime(
        2020, 4, 10, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:

    def create_journal_entry_for_transaction(**context):
        created_date = context["ds"]
        logger.info(f"Created_date: {context['ds']}")
        # find path of template file
        path = None
        for e in pathlib.Path.cwd().rglob("get_daily_transactions.sql"):
            path = e
            break
        if path:
            with path.open() as f:
                sql_template = Template(f.read())
            sql = sql_template.render(created_at=created_date)

            with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PLATFORM",
                ocsp_fail_open=False,
            ) as conn:
                cur = conn.cursor()
                cur.execute(sql)
                data = cur.fetchall()
                df = pd.DataFrame(
                    data,
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
                logger.debug(df.head(10))
                grouped = df.groupby("correlation_guid")
                logger.debug(grouped.head(10))
                succeeded = []
                failed = []
                # login SOAP client
                client = login_client()
                logger.debug(f"Connected: {get_endpoint(client)}")
                data_center_urls = client.service.getDataCenterUrls(netsuite["account"])
                logger.debug(
                    f"Use DataCenterUrl: {data_center_urls.body.getDataCenterUrlsResult.dataCenterUrls.webservicesDomain}"
                )
                for group in grouped:
                    if len(group) == 2:
                        correlation_guid = group[0]
                        try:
                            je_iid = process_grouped_transactions(
                                correlation_guid, group[1], client, created_date
                            )
                            logger.info(
                                f"Journal Entry uploaded: {correlation_guid} - {je_iid}"
                            )
                            succeeded.append(("placeholder", je_iid, correlation_guid))
                        except ValueError as e:
                            logger.error(
                                f"Journal Entry failed: {correlation_guid} - {e}"
                            )
                            failed.append(("placeholder", e, correlation_guid))
                if succeeded:
                    succeeded_values = str(succeeded)[1:-1].replace(
                        "'placeholder'", "current_timestamp::timestamp_ntz"
                    )
                    log_upload = (
                        f"insert into erp.platform.uploaded values {succeeded_values}"
                    )
                    cur.execute(log_upload)
                    logger.info(f"succeeded: {len(succeeded)}")
                if failed:
                    failed_values = str(failed)[1:-1].replace(
                        "'placeholder'", "current_timestamp::timestamp_ntz"
                    )
                    log_failed = (
                        f"insert into erp.platform.failed values {failed_values}"
                    )
                    cur.execute(log_failed)
                    logger.info(f"failed: {len(failed)}")
                cur.commit()

    def create_journal_entry_for_facility_transfer(**context):
        return None

    dag << PythonOperator(
        task_id="get_transactions_by_created_date",
        python_callable=create_journal_entry_for_transaction,
        provide_context=True,
        pool="netsuite_pool",
        dag=dag,
    )
