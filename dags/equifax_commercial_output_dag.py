from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

bucket = "tc-datalake"
commercial_prefix = "equifax_offline_batches/commercial/"

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 1, 7),
    "retries": 0,
}

dag = DAG(
    "equifax_commercial_output_dag", schedule_interval=None, default_args=default_args
)

dt_comm = {
    "customer_ref_no": str,
    "business_name": str,
    "address": str,
    "city": str,
    "province": str,
    "postal_code": str,
    "telephone": str,
    "fax": str,
    "Match Ref Nbr": str,
    "SEQUENCE NUMBER": str,
    "Match Hit Flag": str,
    "Match Subject Nbr": str,
    "Match Company Nbr": int,
    "Match Company Name": str,
    "Match Company Address": str,
    "Match Company City": str,
    "Match Company Province": str,
    "Match Company Postal Code": str,
    "Match Company Phone": str,
    "Primary Subject Nbr": str,
    "Primary Company Nbr": int,
    "Prim Company Name": str,
    "Prim Company Address": str,
    "Prim Company City": str,
    "Prim Company Province": str,
    "Prim Company Postal Code": str,
    "Prim Company Phone": str,
    "Match Comp Year Estab": str,
    "Match Company SIC Code": str,
    "Match Comp SIC Desc English": str,
    "Match Comp SIC Desc French": str,
    "Match Comp NAICS Code": str,
    "Match Comp NAICS Desc English": str,
    "Match Comp NAICS Desc French": str,
    "Prim Comp Year Estab": str,
    "Prim Company SIC Code": str,
    "Prim SIC Desc English": str,
    "Prim SIC Desc French": str,
    "Prim NAICS Code": str,
    "Prim NAICS Desc English": str,
    "Prim NAICS Desc French": str,
    "CI SCORE": str,
    "PI SCORE": str,
    "CDS Model Number": str,
    "CDS Reject Indicat.": str,
    "CDS Reject Code": str,
    "CDS Score Value": str,
    "CDS Reason Code 1": str,
    "CDS Reason Code 2": str,
    "CDS Reason Code 3": str,
    "CDS Reason Code 4": str,
    "BFRS Model Number": str,
    "BFRS Reject Indicat.": str,
    "BFRS Reject Code": str,
    "BFRS Score Value": str,
    "BFRS Reason Code 1": str,
    "BFRS Reason Code 2": str,
    "BFRS Reason Code 3": str,
    "BFRS Reason Code 4": str,
    "CS Model Number": str,
    "CS Reject Indicat.": str,
    "CS Reject Code": str,
    "CS Score Value": str,
    "CS Reason Code 1": str,
    "CS Reason Code 2": str,
    "CS Reason Code 3": str,
    "CS Reason Code 4": str,
    "NBR OF COLL.": int,
    "AMT OF COLL.": int,
    "NBR OF LGL SUITS": int,
    "AMT OF LGL SUITS": int,
    "NBR OF JUDGMENTS": int,
    "AMT OF JUDGMENTS": int,
    "NBR OF RET. CHQS": int,
    "AMT OF RET. CHQS": int,
    "NBR OF OTHER ITEMS": int,
    "AMT OF OTHER ITEMS": int,
    "90 DAY LINES": int,
    "90 DAY TOTAL_BAL": int,
    "90 DAY HIGH CREDIT": int,
    "90 DAY current": int,
    "90 DAY PD1 ODUE": int,
    "90 DAY PD2 ODUE": int,
    "90 DAY PD3 ODUE": int,
    "90 DAY CREDIT LIMIT": int,
    "90 DAY HIGHEST CREDIT": int,
    "ALL PI SCORE": str,
    "ALL LINES": int,
    "ALL TOTAL BAL": int,
    "ALL HIGH CREDIT": int,
    "ALL CURRENT": int,
    "ALL PD1 ODUE": int,
    "ALL PD2 ODUE": int,
    "ALL PD3 ODUE": int,
    "ALL CREDIT LIMIT": int,
    "ALL HIGHEST CREDIT": int,
    "DATE OF OLDEST LINE": str,
    "DATE OF NEWEST LINE": str,
    "TOTAL# OF COLL.": str,
    "AMT PAID ON COLL.": int,
    "# OF COLL-LAST MTH": int,
    "# OF COLL-LAST 2 MTH": int,
    "# OF COLL-LAST 3 MTH": int,
    "# OF COLL > 3 MTH": int,
    "TREND1 YEAR": int,
    "TREND1 QTR": int,
    "TREND1_#_OF_LINES": int,
    "TREND1_TOTAL_OWING": int,
    "TREND1_CURRENT_DUE": int,
    "TREND1_P1_OVERDUE": int,
    "TREND1_P2_OVERDUE": int,
    "TREND1_P3_OVERDUE": int,
    "TREND2_YEAR": int,
    "TREND2_QTR": int,
    "TREND2_#_OF_LINES": int,
    "TREND2_TOTAL_OWING": int,
    "TREND2_CURRENT_DUE": int,
    "TREND2_P1_OVERDUE": int,
    "TREND2_P2_OVERDUE": int,
    "TREND2_P3_OVERDUE": int,
    "TREND3_YEAR": int,
    "TREND3_QTR": int,
    "TREND3_#_OF_LINES": int,
    "TREND3_TOTAL_OWING": int,
    "TREND3_CURRENT_DUE": int,
    "TREND3_P1_OVERDUE": int,
    "TREND3_P2_OVERDUE": int,
    "trend3_p3_overdue": int,
    "trend4_year": int,
    "trend4_qtr": int,
    "TREND4_#_OF_LINES": int,
    "TREND4_TOTAL_OWING": int,
    "TREND4_CURRENT_DUE": int,
    "TREND4_P1_OVERDUE": int,
    "TREND4_P2_OVERDUE": int,
    "TREND4_P3_OVERDUE": int,
    "TREND5_YEAR": int,
    "TREND5_QTR": int,
    "TREND5_#_OF_LINES": int,
    "TREND5_TOTAL_OWING": int,
    "TREND5_CURRENT_DUE": int,
    "TREND5_P1_OVERDUE": int,
    "TREND5_P2_OVERDUE": int,
    "trend5_p3_overdue": int,
    "TREND6_YEAR": int,
    "TREND6_QTR": int,
    "TREND6_#_OF_LINES": int,
    "TREND6_TOTAL_OWING": int,
    "TREND6_CURRENT_DUE": int,
    "TREND6_P1_OVERDUE": int,
    "TREND6_P2_OVERDUE": int,
    "TREND6_P3_OVERDUE": int,
    "TREND7_YEAR": int,
    "TREND7_QTR": int,
    "TREND7_#_OF_LINES": int,
    "TREND7_TOTAL_OWING": int,
    "TREND7_CURRENT_DUE": int,
    "TREND7_P1_OVERDUE": int,
    "TREND7_P2_OVERDUE": int,
    "TREND7_P3_OVERDUE": int,
    "TREND8_YEAR": int,
    "TREND8_QTR": int,
    "TREND8_#_OF_LINES": int,
    "TREND8_TOTAL_OWING": int,
    "TREND8_CURRENT_DUE": int,
    "TREND8_P1_OVERDUE": int,
    "TREND8_P2_OVERDUE": int,
    "TREND8_P3_OVERDUE": int,
    "TREND9_YEAR": int,
    "TREND9_QTR": int,
    "TREND9_#_OF_LINES": int,
    "TREND9_TOTAL_OWING": int,
    "TREND9_CURRENT_DUE": int,
    "TREND9_P1_OVERDUE": int,
    "TREND9_P2_OVERDUE": int,
    "TREND9_P3_OVERDUE": int,
    "INQUIRY_CNT": int,
    "I001 SIC": str,
    "I001 MBR NAME": str,
    "I002 SIC": str,
    "I002 MBR NAME": str,
    "I003 SIC": str,
    "I003 MBR NAME": str,
    "I004 SIC": str,
    "I004 MBR NAME": str,
    "I005 SIC": str,
    "I005 MBR NAME": str,
    "I006 SIC": str,
    "I006 MBR NAME": str,
    "I007 SIC": str,
    "I007 MBR NAME": str,
    "I008 SIC": str,
    "I008 MBR NAME": str,
    "I009 SIC": str,
    "I009 MBR NAME": str,
    "I010 SIC": str,
    "I010 MBR NAME": str,
    "I011 SIC": str,
    "I011 MBR NAME": str,
    "I012 SIC": str,
    "I012 MBR NAME": str,
    "I013 SIC": str,
    "I013 MBR NAME": str,
    "I014 SIC": str,
    "I014 MBR NAME": str,
    "I015 SIC": str,
    "I015 MBR NAME": str,
    "I016 SIC": str,
    "I016 MBR NAME": str,
    "I017 SIC": str,
    "I017 MBR NAME": str,
    "I018 SIC": str,
    "I018 MBR NAME": str,
    "I019 SIC": str,
    "I019 MBR NAME": str,
    "I020 SIC": str,
    "I020 MBR NAME": str,
    "I021 SIC": str,
    "I021 MBR NAME": str,
    "I022 SIC": str,
    "I022 MBR NAME": str,
    "I023 SIC": str,
    "I023 MBR NAME": str,
    "I024 SIC": str,
    "I024 MBR NAME": str,
    "I025 SIC": str,
    "I025 MBR NAME": str,
    "I026 SIC": str,
    "I026 MBR NAME": str,
    "I027 SIC": str,
    "I027 MBR NAME": str,
    "I028 SIC": str,
    "I028 MBR NAME": str,
    "I029 SIC": str,
    "I029 MBR NAME": str,
    "I030 SIC": str,
    "I030 MBR NAME": str,
    "I031 SIC": str,
    "I031 MBR NAME": str,
    "I032 SIC": str,
    "I032 MBR NAME": str,
    "I033 SIC": str,
    "I033 MBR NAME": str,
    "I034 SIC": str,
    "I034 MBR NAME": str,
    "I035 SIC": str,
    "I035 MBR NAME": str,
    "I036 SIC": str,
    "I036 MBR NAME": str,
    "I037 SIC": str,
    "I037 MBR NAME": str,
    "I038 SIC": str,
    "I038 MBR NAME": str,
    "I039 SIC": str,
    "I039 MBR NAME": str,
    "I040 SIC": str,
    "I040 MBR NAME": str,
    "I041 SIC": str,
    "I041 MBR NAME": str,
    "I042 SIC": str,
    "I042 MBR NAME": str,
    "I043 SIC": str,
    "I043 MBR NAME": str,
    "I044 SIC": str,
    "I044 MBR NAME": str,
    "I045 SIC": str,
    "I045 MBR NAME": str,
    "I046 SIC": str,
    "I046 MBR NAME": str,
    "I047 SIC": str,
    "I047 MBR NAME": str,
    "I048 SIC": str,
    "I048 MBR NAME": str,
    "I049 SIC": str,
    "I049 MBR NAME": str,
    "I050 SIC": str,
    "I050 MBR NAME": str,
}
dt_tcap = {
    "customer_ref_no": str,
    "business_name": str,
    "address": str,
    "city": str,
    "province": str,
    "postal_code": str,
    "telephone": str,
    "fax": str,
    "SEQUENCE NUMBER": int,
    "COMPANY NUMBER": int,
    "EFX Reserve AVRO HitFlag": int,
    "Score Code2": str,
    "Score Description2": str,
    "Score value2": str,
    "Reason Code2 1": str,
    "Reason Code2 1 Desc": str,
    "Reason Code2 2": str,
    "Reason Code2 2 Desc": str,
    "Reason Code2 3": str,
    "Reason Code2 3 Desc": str,
    "Reason Code2 4": str,
    "Reason Code2 4 Desc": str,
    "Reason Code2 5": str,
    "Reason Code2 5 Desc": str,
    "Reject Code2": str,
    "Reject Code Desc2": str,
    "Model Number2": int,
    "Score Code3": str,
    "Score Description3": str,
    "Score value3": str,
    "Reason Code3 1": str,
    "Reason Code3 1 Desc": str,
    "Reason Code3 2": str,
    "Reason Code3 2 Desc": str,
    "Reason Code3 3": str,
    "Reason Code3 3 Desc": str,
    "Reason Code3 4": str,
    "Reason Code3 4 Desc": str,
    "Reason Code3 5": str,
    "Reason Code3 5 Desc": str,
    "Reject Code3": str,
    "Reject Code Desc3": str,
    "Model Number3": int,
}


#
# For payment_trend, derogatory_summary_inquiries, inquiries:
# The insertion sql is consist of several sub-queries to be union
# These function handles the tasks to union of up to 50 separate tables created as different set columns from one table
#
def create_payment_trend() -> str:
    sub_queries = []
    for i in range(1, 10):
        sub_queries.append(create_union_level_payment_trend(f"{i}"))
    unioned_sub_query = "\nUNION\n".join(sub_query for sub_query in sub_queries)
    query = f"""INSERT INTO commercial_payment_trends
                SELECT
                    customer_ref_no,
                    import_month,
                    trend_year,
                    trend_qtr,
                    number_of_lines,
                    total_owing,
                    current_due,
                    p1_overdue,
                    p2_overdue,
                    p3_overdue
                FROM ({unioned_sub_query})
                WHERE trend_year != 0"""
    return query


def create_union_level_payment_trend(padded_value: str) -> str:
    query = f"""SELECT
                    customer_ref_no,
                    import_month,
                    trend{padded_value}_year AS trend_year,
                    trend{padded_value}_qtr AS trend_qtr,
                    trend{padded_value}_nb_of_lines AS number_of_lines,
                    trend{padded_value}_total_owing AS total_owing,
                    trend{padded_value}_current_due AS current_due,
                    trend{padded_value}_p1_overdue AS p1_overdue,
                    trend{padded_value}_p2_overdue AS p2_overdue,
                    trend{padded_value}_p3_overdue AS p3_overdue
                FROM
                    equifax_comm"""
    return query


def create_commercial_derogatory_summary_inquiries() -> str:
    name_dict = [
        ["Collections", "coll", "coll_date"],
        ["Legal Suits", "lgl_suits", "suit_date"],
        ["Judgements", "judgments", "judgment_date"],
        ["Returned Cheques", "ret_chqs", "ret_chq_date"],
        ["Other Items", "other_items", "other_item_date"],
    ]
    sub_queries = []
    for i in name_dict:
        sub_queries.append(
            create_union_level_commercial_derogatory_sum(i[0], i[1], i[2])
        )
    unioned_sub_query = "\nUNION\n".join(sub_query for sub_query in sub_queries)
    query = f"""INSERT INTO commercial_derogatory_summary
                SELECT
                    customer_ref_no,
                    import_month,
                    TYPE,
                    count,
                    amount,
                    last_date
                FROM ({unioned_sub_query}) AS data"""
    return query


def create_union_level_commercial_derogatory_sum(a: str, b: str, c: str) -> str:
    query = f"""SELECT
                    customer_ref_no,
                    import_month,
                    '{a}' AS TYPE,
                    nbr_of_{b} AS count,
                    amt_of_{b} AS amount,
                    last_{c} AS last_date
                FROM
                    equifax_comm
                WHERE
                    match_hit_flag = '1'"""
    return query


def create_commercial_inquiries() -> str:
    sub_queries = []
    for i in range(1, 51):
        sub_queries.append(create_union_level_commercial_inquiries(f"{i:03}"))
    unioned_sub_query = "\nUNION\n".join(sub_query for sub_query in sub_queries)
    query = f"""INSERT INTO commercial_inquiries
            SELECT
                customer_ref_no,
                import_month,
                row_number() OVER (PARTITION BY customer_ref_no ORDER BY customer_ref_no) AS inquiry_number,
                inquiry_date,
                inquirer,
                inquirer_sic_code
            FROM ({unioned_sub_query})
            WHERE inquiry_date IS NOT NULL;"""
    return query


def create_union_level_commercial_inquiries(padded_value: str) -> str:
    query = f"""SELECT
                    customer_ref_no,
                    import_month,
                    i{padded_value}_date AS inquiry_date,
                    i{padded_value}_mbr_name AS inquirer,
                    i{padded_value}_sic AS inquirer_sic_code
                FROM
                    equifax_comm"""
    return query


commercial_derogatory_summary = create_commercial_derogatory_summary_inquiries()
commercial_payment_trends = create_payment_trend()
commercial_inquiries = create_commercial_inquiries()

commercial_tradeline_summary = """
    INSERT INTO commercial_tradeline_summary
    SELECT
        customer_ref_no,
        import_month,
        90_day_lines,
        90_day_total_bal,
        90_day_high_credit,
        90_day_current,
        90_day_pd1_odue,
        90_day_pd2_odue,
        90_day_pd3_odue,
        90_day_credit_limit,
        90_day_highest_credit,
        all_pi_score,
        all_lines,
        all_total_bal,
        all_high_credit,
        all_current,
        all_pd1_odue,
        all_pd2_odue,
        all_pd3_odue,
        all_credit_limit,
        all_highest_credit,
        date_of_oldest_line,
        date_of_newest_line,
        totalnb_of_coll,
        amt_paid_on_coll,
        nb_of_coll_last_mth,
        nb_of_coll_last_2_mth,
        nb_of_coll_last_3_mth,
        nb_of_coll_more_3_mth
    FROM
        equifax_comm
    WHERE
        match_hit_flag = '1'
"""
commercial_information = """
INSERT INTO commercial_information
SELECT
    customer_ref_no,
    import_month,
    business_name,
    address,
    city,
    province,
    postal_code,
    telephone,
    fax,
    match_ref_nbr,
    sequence_number,
    match_hit_flag,
    match_subject_nbr,
    match_company_nbr,
    match_company_name,
    match_company_address,
    match_company_city,
    match_company_province,
    match_company_postal_code,
    match_company_phone,
    primary_subject_nbr,
    prim_company_name,
    prim_company_address,
    prim_company_city,
    prim_company_province,
    prim_company_postal_code,
    prim_company_phone,
    match_company_open_date,
    match_comp_year_estab,
    match_company_sic_code,
    match_comp_sic_desc_english,
    match_comp_sic_desc_french,
    prim_company_open_date,
    prim_comp_year_estab,
    prim_company_sic_code,
    prim_sic_desc_english,
    prim_sic_desc_french,
    prim_naics_code,
    prim_naics_desc_english,
    prim_naics_desc_french,
    imported_file_name
FROM
    equifax_comm
"""
commercial_scores = """
    INSERT INTO commercial_scores
    SELECT
        a.customer_ref_no,
        a.import_month,
        a.ci_score,
        a.pi_score,
        a.cds_score_value,
        a.bfrs_score_value,
        a.cs_score_value,
        b.score_value2 AS cds2_score_value,
        b.score_value3 AS bfrs2_score_value
    FROM
        equifax_comm AS a
        LEFT JOIN equifax_tcap AS b ON a.customer_ref_no = b.customer_ref_no
            AND a.import_month = b.import_month
    WHERE
        a.match_hit_flag = '1';
"""

snowflake_hook = BaseHook.get_connection("airflow_production")
# redshift_hook = PostgresHook(postgres_conn_id="redshift_tc_dw")
aws_hook = AwsHook(aws_conn_id="s3_equifax")
aws_credentials = aws_hook.get_credentials()


def convert_csv(type_name: str) -> None:
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )
    keys = client.list_objects(Bucket=bucket, Prefix=commercial_prefix, Delimiter="/")
    threshold = datetime.now(timezone.utc) + timedelta(days=-60)
    for key in keys["CommonPrefixes"]:
        objects = client.list_objects(
            Bucket=bucket, Prefix=key["Prefix"], Delimiter="/"
        )
        if "Contents" in objects:
            for content in objects["Contents"]:
                last_modified = content["LastModified"]
                file_name = (
                    content["Key"].replace(bucket, "").replace(commercial_prefix, "")
                )
                if not file_name.endswith("/") and threshold < last_modified:
                    if type_name == "comm" and "comm" in file_name.lower():
                        path = content["Key"]
                    elif "comm" not in file_name.lower() and type_name != "comm":
                        path = content["Key"]

    if path:
        import_file = path.split("/")[-1]
        import_month = path.split("/")[-2]
        print(f"Loading from s3://{bucket}/{path}")
        obj = client.get_object(Bucket=bucket, Key=path)
        if type_name == "comm":
            dt = dt_comm
        else:
            dt = dt_tcap
        df = pd.read_csv(
            obj["Body"], delimiter=",", encoding="ISO-8859-1", quotechar='"', dtype=dt
        )
        df["imported_file_name"] = import_file
        df["import_month"] = import_month
        print(f"{len(df)} rows loaded from origin {type_name} csv")
        df.to_csv(f"equifax_{type_name}.csv", index=False, sep="\t")
        with open(f"equifax_{type_name}.csv", "rb") as file:
            print(
                f"Upload converted csv to S3: {commercial_prefix}equifax_{type_name}.csv"
            )
            client.upload_fileobj(
                file, bucket, f"{commercial_prefix}equifax_{type_name}.csv"
            )


def convert_csv_comm() -> None:
    convert_csv("comm")


def convert_csv_tcap() -> None:
    convert_csv("tcap")


def copy_to_snowflake(type_name: str) -> None:
    sql_create_stage = f"""CREATE OR REPLACE TABLE equifax_{type_name}_staging like equifax_{type_name}"""
    sql_copy_stage = f"""COPY INTO equifax_{type_name}_staging 
                            FROM S3://{bucket}/{commercial_prefix}equifax_{type_name}.csv 
                            CREDENTIALS = (
                                aws_key_id='{aws_credentials.access_key}',
                                aws_secret_key='{aws_credentials.secret_key}')
                                FILE_FORMAT=(field_delimiter='\t', skip_header=1)"""
    sql_delete_dups = f"""MERGE INTO equifax_{type_name}_staging tab2 
                            USING equifax_{type_name} tab1 
                            ON tab2.CUSTOMER_REF_NO = tab1.CUSTOMER_REF_NO 
                                AND tab2.import_month = tab1.import_month
                            WHEN MATCHED THEN DELETE"""
    sql_insert = (
        f"INSERT INTO equifax_{type_name} SELECT * FROM equifax_{type_name}_staging"
    )
    con = snowflake.connector.connect(
        user=snowflake_hook.login,
        password=snowflake_hook.password,
        account=snowflake_hook.host,
        warehouse="ETL",
        database="EQUIFAX",
        schema="public",
        ocsp_fail_open=False,
    )

    cs = con.cursor()
    cs.execute(sql_create_stage)
    print(f"Copy to {type_name} staging table")
    cs.execute(sql_copy_stage)
    print(f"Delete duplicates in {type_name} staging table")
    cs.execute(sql_delete_dups)
    print(f"Insert rows in {type_name} table")
    cs.execute(sql_insert)
    print(f"Truncate {type_name} staging table")
    cs.execute(f"TRUNCATE TABLE equifax_{type_name}_staging")
    con.close()


def copy_to_snowflake_comm() -> None:
    copy_to_snowflake("comm")


def copy_to_snowflake_tcap() -> None:
    copy_to_snowflake("tcap")


# def copy_to_redshift_comm():
#     copy_to_redshift("comm")


# def copy_to_redshift_tcap():
#     copy_to_redshift("tcap")


# def copy_to_redshift(type_name):
#     sql_create_staging = f"""create table if not exists equifax_{type_name}_staging (like equifax_{type_name});
#                             """
#     sql_drop_staging = f"""drop table equifax_{type_name}_staging;
#                                 """
#     sql_copy_staging = f"""COPY equifax_{type_name}_staging
#                             FROM 's3://{bucket}/{commercial_prefix}equifax_{type_name}.csv'
#                             ACCESS_KEY_ID '{aws_credentials.access_key}'
#                             SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
#                             CSV
#                             DELIMITER '\t'
#                             DATEFORMAT 'auto'
#                             TIMEFORMAT 'auto'
#                             TRUNCATECOLUMNS
#                             TRIMBLANKS
#                             IGNOREHEADER 1"""
#     sql_delete_dups = f"""DELETE FROM equifax_{type_name}_staging
#                             USING equifax_{type_name}
#                             where equifax_{type_name}_staging.customer_ref_no = equifax_{type_name}.customer_ref_no
#                                 AND equifax_{type_name}_staging.import_month = equifax_{type_name}.import_month"""
#     sql_insert = (
#         f"INSERT INTO equifax_{type_name} SELECT * FROM equifax_{type_name}_staging"
#     )
#     conn = redshift_hook.get_conn()
#
#     cs = conn.cursor()
#     cs.execute("set search_path='equifax'")
#     cs.execute(sql_create_staging)
#     conn.commit()
#     print(f"Create staging table: equifax_{type_name}_staging")
#     print(f"Copy to {type_name} staging table")
#     cs.execute(sql_copy_staging)
#     print(f"Delete duplicates in {type_name} staging table")
#     cs.execute(sql_delete_dups)
#     print(f"Insert rows in {type_name} table")
#     cs.execute(sql_insert)
#     print(f"Drop staging table: equifax_{type_name}_staging")
#     cs.execute(sql_drop_staging)
#     conn.commit()
#     conn.close()


def clean_up_converted_file() -> None:
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )
    response = client.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [
                {"Key": f"{commercial_prefix}equifax_comm.csv"},
                {"Key": f"{commercial_prefix}equifax_tcap.csv"},
            ]
        },
    )
    print(f"Clean up {commercial_prefix}equifax_comm.csv")
    print(f"Clean up {commercial_prefix}equifax_tcap.csv")
    print(f"Status: {response}")


def truncate_commercial_tables() -> None:
    con = snowflake.connector.connect(
        user=snowflake_hook.login,
        password=snowflake_hook.password,
        account=snowflake_hook.host,
        warehouse="ETL",
        database="EQUIFAX",
        schema="public",
        ocsp_fail_open=False,
    )

    cs = con.cursor()
    print("Truncate commercial tables")
    for table_name in commercial_table_sqls:
        cs.execute(f"TRUNCATE TABLE {table_name}")
    con.close()


# def truncate_redshift_commercial_tables():
#     con = redshift_hook.get_conn()
#
#     cs = con.cursor()
#     cs.execute(f"set search_path='equifax'")
#     print(f"Truncate commercial tables")
#     for table_name in commercial_table_sqls:
#         cs.execute(f"TRUNCATE TABLE {table_name}")
#     con.close()


commercial_table_sqls = {
    "commercial_derogatory_summary": commercial_derogatory_summary,
    "commercial_payment_trends": commercial_payment_trends,
    "commercial_inquiries": commercial_inquiries,
    "commercial_tradeline_summary": commercial_tradeline_summary,
    "commercial_information": commercial_information,
    "commercial_scores": commercial_scores,
}


def compute_commercial_table(key: str) -> SnowflakeOperator:
    return SnowflakeOperator(
        task_id=key,
        sql=commercial_table_sqls[key],
        snowflake_conn_id="snowflake_default",
        account=snowflake_hook.host,
        warehouse="ETL",
        database="EQUIFAX",
        schema="public",
        dag=dag,
    )


# def compute_redshift_commercial_table(key):
#     return PostgresOperator(
#         task_id=f"redshift_{key}",
#         sql=["set search_path='equifax'", commercial_table_sqls[key]],
#         postgres_conn_id="redshift_tc_dw",
#         database="tc_datawarehouse",
#         dag=dag,
#     )


task_convert_csv_comm = PythonOperator(
    task_id="convert_csv_comm", python_callable=convert_csv_comm, dag=dag
)

task_convert_csv_tcap = PythonOperator(
    task_id="convert_csv_tcap", python_callable=convert_csv_tcap, dag=dag
)

task_copy_to_snowflake_comm = PythonOperator(
    task_id="copy_to_snowflake_comm",
    python_callable=copy_to_snowflake_comm,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

task_copy_to_snowflake_tcap = PythonOperator(
    task_id="copy_to_snowflake_tcap",
    python_callable=copy_to_snowflake_tcap,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

# task_copy_to_redshift_comm = PythonOperator(
#     task_id="copy_to_redshift_comm",
#     python_callable=copy_to_redshift_comm,
#     trigger_rule=TriggerRule.NONE_FAILED,
#     dag=dag,
# )
#
# task_copy_to_redshift_tcap = PythonOperator(
#     task_id="copy_to_redshift_tcap",
#     python_callable=copy_to_redshift_tcap,
#     trigger_rule=TriggerRule.NONE_FAILED,
#     dag=dag,
# )

task_clean_up_converted_file = PythonOperator(
    task_id="clean_up_converted_file",
    python_callable=clean_up_converted_file,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

task_truncate_commercial_tables = PythonOperator(
    task_id="truncate_commercial_tables",
    python_callable=truncate_commercial_tables,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

# task_truncate_redshift_commercial_tables = PythonOperator(
#     task_id="truncate_redshift_commercial_tables",
#     python_callable=truncate_redshift_commercial_tables,
#     trigger_rule=TriggerRule.NONE_FAILED,
#     dag=dag,
# )
# delete from tab1 using tab2 where tab1.k = tab2.k
#
task_convert_csv_comm >> task_copy_to_snowflake_comm >> task_clean_up_converted_file
task_convert_csv_tcap >> task_copy_to_snowflake_tcap >> task_clean_up_converted_file
task_clean_up_converted_file >> task_truncate_commercial_tables
for key in commercial_table_sqls:
    task_truncate_commercial_tables >> compute_commercial_table(key)
    # task_truncate_redshift_commercial_tables >> compute_redshift_commercial_table(key)
