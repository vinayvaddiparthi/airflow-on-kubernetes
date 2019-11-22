import json
from datetime import datetime

import pandas as pd
import requests
import snowflake.connector
import xmltodict
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from netsuite_extras import netsuite_helper

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 20),
    "retries": 0,
}

aws_hook = AwsHook(aws_conn_id="s3_conn_id")
aws_credentials = aws_hook.get_credentials()

with DAG("netsuite_journal_entry", schedule_interval=None, default_args=default_args) as dag:
    snowflake_hook = BaseHook.get_connection("snowflake_erp")


    def get_count(cd, cb):
        trim_cd = cd.replace('-', '')
        sql = f"""
                create or replace table cd_temp_{trim_cd} as
                select distinct tli.transaction_date__c td, count(*) over (partition by td)
                from salesforce.sfoi.c2g__codatransactionlineitem__c as tli
                left join salesforce.sfoi.c2g__codajournal__c as je on je.c2g__transaction__c = tli.c2g__transaction__c
                where to_date(tli.createddate) = '{cd}' and tli.createdbyid = '{cb}'
                order by td desc;"""
        with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                ocsp_fail_open=False) as conn:
            cur = conn.cursor()
            cur.execute(sql)


    def tag_tli_with_process(cd, cb):
        trim_cd = cd.replace('-', '')
        sql = f"""
                select * from salesforce.sfoi.cd_temp_{trim_cd}"""
        with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                ocsp_fail_open=False) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            tds = [x[0] for x in cur.fetchall()]
            for td in tds:
                td_str = td.strftime("%Y-%m-%d")
                mp_nsf_sql = f"""
                            select tli.general_ledger_account_number__c    as old_gl,
                                   tli.transaction_date__c                 as transaction_date,
                                   tli.CREATEDDATE                         as created_date,
                                   tli.id                                  as id,
                                   tli.c2g__linedescription__c             as line_description,
                                   tli.c2g__linereference__c               as line_reference,
                                   je.c2g__journaldescription__c           as journal_description,
                                   tli.debit__c                            as debit,
                                   tli.credit__c                           as credit,
                                   c2g__homevalue__c                       as home_value,
                                   'Manual Payment cash collection of NSF' as process
                            from sfoi.c2g__codatransactionlineitem__c as tli
                            left join sfoi.c2g__codajournal__c as je on je.c2g__transaction__c = tli.c2g__transaction__c
                            where tli.createdbyid = '{cb}'
                              and created_date = '{cd}'
                              and transaction_date = '{td_str}'
                              and (
                                    (tli.general_ledger_account_number__c like '1008' and tli.debit__c > '0'
                                        and tli.c2g__linereference__c like '%NSF Resubmit%')
                                    or
                                    (tli.credit__c < '0' and tli.c2g__linedescription__c like '%NSF Resubmit%'
                                        and tli.general_ledger_account_number__c in ('2103', '1115', '1116'))
                                );
                """


    def error_mp(result):
        print('Nothing to check for now')
        return None


    def get_manual_payments(**kwargs):
        scheduled_date = kwargs['prev_ds']
        execution_time = kwargs['ts']
        process_mp = f"""
        select coa.new_account,
               tli.transaction_date__c,
               tli.c2g__linedescription__c,
               tli.c2g__linereference__c,
               je.c2g__journaldescription__c,
               sum(tli.debit__c)      as debit,
               sum(tli.credit__c)     as credit,
               sum(c2g__homevalue__c) as balance,
               coa.subsidiary as subsidiary,
               aid.internal_id
        from salesforce.sfoi.c2g__codatransactionlineitem__c as tli
                 left join salesforce.sfoi.c2g__codajournal__c as je on je.c2g__transaction__c = tli.c2g__transaction__c
                 left join erp.public.chart_of_account as coa
                           on to_char(tli.general_ledger_account_number__c) = coa.old_account
                 left join erp.public.account_internal_ids as aid
                           on to_char(aid.account) = coa.new_account
        where (tli.c2g__linereference__c like '%Collection%'
            or tli.c2g__linedescription__c like '%Collection%')
          and (tli.c2g__linereference__c not like '%Loan Collection%'
            or tli.c2g__linedescription__c not like '%Loan Collection%')
          and transaction_date__c = '{scheduled_date}'
          and (general_ledger_account_number__c = '2103'
            or (general_ledger_account_number__c = '1008'
                and tli.c2g__linereference__c not like '%EQB Transfer%')
            or general_ledger_account_number__c = '1115'
            or general_ledger_account_number__c = '1116')
        group by 1, 2, 3, 4, 5, 9, 10
        order by 8 desc
        """
        with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                ocsp_fail_open=False) as conn:
            cur = conn.cursor()
            cur.execute(process_mp)
            data = cur.fetchall()
            if len(data) == 0:
                log_je_history('MP', False, '', 'No entry today', execution_time, scheduled_date, None)
                return 'empty'

            df = pd.DataFrame(data)
            df.columns = ['gl_account', 'transaction_date', 'line_description', 'line_reference',
                          'journal_description', 'debit', 'credit', 'balance', 'subsidiary', 'internal_id']
            result = df.to_dict('records')

            if error_mp(result):
                print(error_mp(result))
            else:
                netsuite_hook = BaseHook.get_connection("netsuite_sandbox")
                email = netsuite_hook.login
                password = netsuite_hook.password
                account = netsuite_hook.schema
                app_id = json.loads(netsuite_hook.extra)["app_id"]
                endpoint = netsuite_hook.host
                print(f"{email},\n {password}, \n{account}, \n{app_id}, \n{endpoint}")
                payload = netsuite_helper.get_manual_payment_payload(scheduled_date, result, email, password, account,
                                                                     app_id)
                headers = {'content-type': 'application/xml',
                           'soapaction': 'Add',
                           'cache-control': 'no-cache'}
                r = requests.post(endpoint, headers=headers, data=payload)
                j = xmltodict.parse(r.content)
                write_response = j["soapenv:Envelope"]["soapenv:Body"]["addResponse"]["writeResponse"]
                if write_response["platformCore:status"]["@isSuccess"] == "true":
                    try:
                        je_internal_id = \
                            j["soapenv:Envelope"]["soapenv:Body"]["addResponse"]["writeResponse"]["baseRef"][
                                "@internalId"]
                        log_je_history('MP', True, je_internal_id, '', execution_time, scheduled_date, data)
                    except KeyError as e:
                        print(e)
                        raise e
                else:
                    try:
                        code = write_response["platformCore:status"]['platformCore:statusDetail']['platformCore:code']
                        message = write_response["platformCore:status"]['platformCore:statusDetail'][
                            'platformCore:message']
                        error_message = code + ":" + message
                        log_je_history('MP', False, '', error_message, execution_time, scheduled_date, data)
                    except Exception as e:
                        print(e)
                        raise e


    def log_je_history(type, success, je_internal_id, error, execution_time, scheduled_date, data):
        success = 'TRUE' if success else 'FALSE'
        with snowflake.connector.connect(
                user=snowflake_hook.login,
                password=snowflake_hook.password,
                account=snowflake_hook.host,
                warehouse="ETL",
                database="ERP",
                schema="PUBLIC",
                ocsp_fail_open=False) as conn:
            cur = conn.cursor()
            cur.execute(
                f"insert into erp.public.journal_entry_status values ('{type}', '{execution_time}', '{je_internal_id}', {success} , '{error}', '{scheduled_date}')")
            if data:
                data_l = []
                for row in data:
                    row_l = list(row)
                    row_l[-1] = je_internal_id
                    data_l.append(tuple(row_l))
                print(",".join(data_l))
                cur.execute(
                    f"insert into erp.public.journal_entry_history values {data_l}")


    dag << PythonOperator(
        task_id=f"get_journal_entry_mp",
        python_callable=get_manual_payments,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
