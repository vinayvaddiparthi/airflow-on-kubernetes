from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 21),
    "retries": 0,
}

with DAG(
    "sas_to_snowflake_dag", schedule_interval="0 9 * * *", default_args=default_args
) as dag:
    sobjects = [
        "account_owner",
        "account_status",
        "contract_history",
        "days_non_payment",
        "days_past_due",
        "loan_account",
        "loan_account_summary",
        "principal_balance",
        "total_balance",
    ]

    snowflake_hook = BaseHook.get_connection("snowflake_sas")
    aws_hook = AwsHook(aws_conn_id="s3_conn_id")
    aws_credentials = aws_hook.get_credentials()


    def create_request_file():
        import pandas as pd

        header = f"BHDR-EQUIFAX{datetime.now().strftime('%Y%m%d%H%M%S')}ADVITFINSCOREDA2"
        trailer = f"BTRL-EQUIFAX{datetime.now().strftime('%Y%m%d%H%M%S')}ADVITFINSCOREDA2"
        with snowflake.connector.connect(
                user="xzhang",
                password="f951up*LgL9J3&V83nq7*6XJp",
                account="thinkingcapital.ca-central-1.aws",
                warehouse="ETL",
                database="EQUIFAX",
                schema="REQUEST",
                ocsp_fail_open=False) as conn:
            cur = conn.cursor()
            cur.execute("select * from equifax.request.consumer_batch_fix")
            data = cur.fetchall()
            df = pd.DataFrame(data)
            print(df.head())
            print('load table in to df')
            df.to_csv(r'temp_formatted.csv', header=None, index=None, sep='\t', encoding='utf-8')
            with open('temp_formatted.csv', mode='r', encoding='utf-8') as file_in:
                text = file_in.read()
                text = text.replace('\t', '')
                with open('textformatted.txt', mode='w') as file_out:
                    file_out.writelines(header)
                    file_out.write('\n')
                    file_out.write(text)
                    file_out.writelines(trailer)
                    file_out.writelines(str(len(df)).zfill(8))
                with open("textformatted.txt", mode='r') as file:
                    lines = file.readlines()
                    c = 0
                    for line in lines:
                        if len(line) != 221:
                            print(f"{line}")
                            c += 1
                    print(f"wrong lines: {c}")
                    client = boto3.client(
                        "s3",
                        aws_access_key_id="AKIAS5TFJL6B73Q362M5",
                        aws_secret_access_key="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"
                    )
                    client.upload_fileobj(
                        file, bucket, f"{prefix}/{file_name}"
                    )

    create_request = PythonOperator(
        task_id=f"snowflake_copy_tli_1",
        python_callable=copy_tli_to_snowflake,
        op_kwargs={"part": 1},
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    dag << create_request
