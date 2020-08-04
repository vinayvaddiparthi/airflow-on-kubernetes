from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import snowflake.connector
import tempfile
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

bucket = 'tc-datalake'
prefix = 'equifax_offline_batch/consumer/output/'
result_dict_1 = {
    "customer_reference_number": 12,
    "last_name": 25,
    "first_name": 15,
    "middle_name": 10,
    "filler1": 17,
    "sin": 9,
    "dob_text": 6,
}
result_dict_2 = {
    "address": 30,
    "city": 20,
    "province": 2,
    "postal_code": 6,
    "account_number": 15,
    "filler2": 28,
    "ers_classification_ind": 1,
    "filler3": 1,
    "ers_reject_code": 3,
    "filler4": 5,
    "ers_reason_code_1": 2,
    "ers_reason_code_2": 2,
    "ers_reason_code_3": 2,
    "ers_reason_code_4": 2,
    "ers_score": 3,
    "filler5": 4,
    "fico_classification_ind": 1,
    "filler6": 1,
    "fico_reject_code": 3,
    "filler7": 5,
    "fico_reason_code_1": 2,
    "fico_reason_code_2": 2,
    "fico_reason_code_3": 2,
    "fico_reason_code_4": 2,
    "fico_score": 3,
    "filler8": 4,
    "equifax_sequence_number": 7,
    "OTXX001": 1,
    "OTXX002": 1,
    "OTXX003": 1,
    "OTXX004": 5,
    "OTXX005": 1,
    "filler9": 15,
    "filler10": 50,
    "OTXX008": 1,
    "OTXX009": 1,
    "OTXX010": 5,
    "OTXX011": 3,
    "filler11": 5,
    "OTXX013": 5,
    "PRXX001": 5,
    "PRXX002": 5,
    "PRXX006": 3,
    "PRXX007": 5,
    "PRXX008": 5,
    "PRXX009": 5,
    "PRXX010": 5,
    "PRXX011": 5,
    "PRXX012": 5,
    "PRXX013": 5,
    "PRXX014": 6,
    "PRXX015": 5,
    "PRXX016": 6,
    "PRXX017": 9,
    "PRXX018": 5,
    "PRXX019": 5,
    "PRXX020": 5,
    "PRXX021": 5,
    "PRXX022": 5,
    "PRXX023": 5,
    "PRXX024": 5,
    "PRXX025": 5,
    "PRXX026": 5,
    "PRXX027": 5,
    "PRXX028": 5,
    "PRXX029": 5,
    "PRXX030": 5,
    "PRXX031": 5,
    "PRXX032": 5,
    "PRXX033": 5,
    "PRXX034": 5,
    "PRXX035": 5,
    "PRXX036": 5,
    "PRXX037": 9,
    "PRXX038": 9,
    "PRXX039": 6,
    "PRXX040": 5,
    "PRXX041": 5,
    "PRXX042": 5,
    "PRXX043": 5,
    "PRXX044": 6,
    "PRXX045": 5,
    "PRXX046": 5,
    "PRXX047": 5,
    "PRXX048": 5,
    "PRXX049": 5,
    "PRXX050": 5,
    "PRXX051": 5,
    "PRXX052": 5,
    "PRXX053": 5,
    "PRXX054": 5,
    "PRXX055": 5,
    "PRXX056": 5,
    "PRXX057": 5,
    "PRXX058": 5,
    "PRXX059": 5,
    "PRXX060": 9,
    "PRXX061": 9,
    "PRXX062": 9,
    "PRXX063": 5,
    "PRXX064": 5,
    "PRXX065": 5,
    "INQAL003": 5,
    "INQAL004": 5,
    "INQAL005": 5,
    "INQAL006": 5,
    "INQAL007": 5,
    "INQAL008": 5,
    "INQAL009": 6,
    "INQAL010": 5,
    "INQAM003": 5,
    "INQAM004": 5,
    "INQAM005": 5,
    "INQAM006": 5,
    "INQAM007": 5,
    "INQAM008": 5,
    "INQAM009": 6,
    "INQAM010": 5,
    "INQMG003": 5,
    "INQMG004": 5,
    "INQMG005": 5,
    "INQMG006": 5,
    "INQMG007": 5,
    "INQMG008": 5,
    "INQMG009": 6,
    "INQMG010": 5,
    "INQBK003": 5,
    "INQBK004": 5,
    "INQBK005": 5,
    "INQBK006": 5,
    "INQBK007": 5,
    "INQBK008": 5,
    "INQBK009": 6,
    "INQBK010": 5,
    "INQCU003": 5,
    "INQCU004": 5,
    "INQCU005": 5,
    "INQCU006": 5,
    "INQCU007": 5,
    "INQCU008": 5,
    "INQCU009": 6,
    "INQCU010": 5,
    "INQNC003": 5,
    "INQNC004": 5,
    "INQNC005": 5,
    "INQNC006": 5,
    "INQNC007": 5,
    "INQNC008": 5,
    "INQNC009": 6,
    "INQNC010": 5,
    "INQAF003": 5,
    "INQAF004": 5,
    "INQAF005": 5,
    "INQAF006": 5,
    "INQAF007": 5,
    "INQAF008": 5,
    "INQAF009": 6,
    "INQAF010": 5,
    "INQPF003": 5,
    "INQPF004": 5,
    "INQPF005": 5,
    "INQPF006": 5,
    "INQPF007": 5,
    "INQPF008": 5,
    "INQPF009": 6,
    "INQPF010": 5,
    "INQSF003": 5,
    "INQSF004": 5,
    "INQSF005": 5,
    "INQSF006": 5,
    "INQSF007": 5,
    "INQSF008": 5,
    "INQSF009": 6,
    "INQSF010": 5,
    "INQRT003": 5,
    "INQRT004": 5,
    "INQRT005": 5,
    "INQRT006": 5,
    "INQRT007": 5,
    "INQRT008": 5,
    "INQRT009": 6,
    "INQRT010": 5,
    "INQRD003": 5,
    "INQRD004": 5,
    "INQRD005": 5,
    "INQRD006": 5,
    "INQRD007": 5,
    "INQRD008": 5,
    "INQRD009": 6,
    "INQRD010": 5,
    "INQTE003": 5,
    "INQTE004": 5,
    "INQTE005": 5,
    "INQTE006": 5,
    "INQTE007": 5,
    "INQTE008": 5,
    "INQTE009": 6,
    "INQTE010": 5,
    "INQBD003": 5,
    "INQBD004": 5,
    "INQBD005": 5,
    "INQBD006": 5,
    "INQBD007": 5,
    "INQBD008": 5,
    "INQBD009": 6,
    "INQBD010": 5,
    "INQCL003": 5,
    "INQCL004": 5,
    "INQCL005": 5,
    "INQCL006": 5,
    "INQCL007": 5,
    "INQCL008": 5,
    "INQCL009": 6,
    "INQCL010": 5,
    "TCAL011": 5,
    "TCAL014": 5,
    "TCAL015": 5,
    "TCAL016": 5,
    "TCAL020": 5,
    "TCAL022": 5,
    "TCAL023": 5,
    "TCAL029": 5,
    "TCAL030": 5,
    "TCAL031": 9,
    "TCAL034": 9,
    "TCAL035": 9,
    "TCAL036": 6,
    "TCAL055": 5,
    "TCAL060": 5,
    "TCAL061": 5,
    "TCAL062": 5,
    "TCAL063": 5,
    "TCAL064": 5,
    "TCAL065": 5,
    "TCAL068": 5,
    "TCAL069": 5,
    "TCAL070": 5,
    "TCAL071": 5,
    "TCAL072": 5,
    "TCAL073": 5,
    "TCAL084": 5,
    "TCAL085": 5,
    "TCAL086": 5,
    "TCAL087": 5,
    "TCAL088": 5,
    "TCAL089": 5,
    "TCAL092": 5,
    "TCAL093": 5,
    "TCAL094": 5,
    "TCAL095": 5,
    "TCAL096": 5,
    "TCAL097": 5,
    "TCAL100": 5,
    "TCAL108": 5,
    "TCAM011": 5,
    "TCAM014": 5,
    "TCAM015": 5,
    "TCAM016": 5,
    "TCAM020": 5,
    "TCAM022": 5,
    "TCAM023": 5,
    "TCAM029": 5,
    "TCAM030": 5,
    "TCAM031": 9,
    "TCAM034": 9,
    "TCAM035": 9,
    "TCAM036": 6,
    "TCAM055": 5,
    "TCAM060": 5,
    "TCAM061": 5,
    "TCAM062": 5,
    "TCAM063": 5,
    "TCAM064": 5,
    "TCAM065": 5,
    "TCAM068": 5,
    "TCAM069": 5,
    "TCAM070": 5,
    "TCAM071": 5,
    "TCAM072": 5,
    "TCAM073": 5,
    "TCAM084": 5,
    "TCAM085": 5,
    "TCAM086": 5,
    "TCAM087": 5,
    "TCAM088": 5,
    "TCAM089": 5,
    "TCAM092": 5,
    "TCAM093": 5,
    "TCAM094": 5,
    "TCAM095": 5,
    "TCAM096": 5,
    "TCAM097": 5,
    "TCAM100": 5,
    "TCAM108": 5,
    "TCIN011": 5,
    "TCIN014": 5,
    "TCIN015": 5,
    "TCIN016": 5,
    "TCIN020": 5,
    "TCIN022": 5,
    "TCIN023": 5,
    "TCIN029": 5,
    "TCIN030": 5,
    "TCIN031": 9,
    "TCIN034": 9,
    "TCIN035": 9,
    "TCIN036": 6,
    "TCIN055": 5,
    "TCIN060": 5,
    "TCIN061": 5,
    "TCIN062": 5,
    "TCIN063": 5,
    "TCIN064": 5,
    "TCIN065": 5,
    "TCIN068": 5,
    "TCIN069": 5,
    "TCIN070": 5,
    "TCIN071": 5,
    "TCIN072": 5,
    "TCIN073": 5,
    "TCIN084": 5,
    "TCIN085": 5,
    "TCIN086": 5,
    "TCIN087": 5,
    "TCIN088": 5,
    "TCIN089": 5,
    "TCIN092": 5,
    "TCIN093": 5,
    "TCIN094": 5,
    "TCIN095": 5,
    "TCIN096": 5,
    "TCIN097": 5,
    "TCIN100": 5,
    "TCIN108": 5,
    "TCOP011": 5,
    "TCOP014": 5,
    "TCOP015": 5,
    "TCOP016": 5,
    "TCOP020": 5,
    "TCOP022": 5,
    "TCOP023": 5,
    "TCOP029": 5,
    "TCOP030": 5,
    "TCOP031": 9,
    "TCOP034": 9,
    "TCOP035": 9,
    "TCOP036": 6,
    "TCOP055": 5,
    "TCOP060": 5,
    "TCOP061": 5,
    "TCOP062": 5,
    "TCOP063": 5,
    "TCOP064": 5,
    "TCOP065": 5,
    "TCOP068": 5,
    "TCOP069": 5,
    "TCOP070": 5,
    "TCOP071": 5,
    "TCOP072": 5,
    "TCOP073": 5,
    "TCOP084": 5,
    "TCOP085": 5,
    "TCOP086": 5,
    "TCOP087": 5,
    "TCOP088": 5,
    "TCOP089": 5,
    "TCOP092": 5,
    "TCOP093": 5,
    "TCOP094": 5,
    "TCOP095": 5,
    "TCOP096": 5,
    "TCOP097": 5,
    "TCOP100": 5,
    "TCOP108": 5,
    "TCRE011": 5,
    "TCRE014": 5,
    "TCRE015": 5,
    "TCRE016": 5,
    "TCRE020": 5,
    "TCRE022": 5,
    "TCRE023": 5,
    "TCRE029": 5,
    "TCRE030": 5,
    "TCRE031": 9,
    "TCRE034": 9,
    "TCRE035": 9,
    "TCRE036": 6,
    "TCRE055": 5,
    "TCRE060": 5,
    "TCRE061": 5,
    "TCRE062": 5,
    "TCRE063": 5,
    "TCRE064": 5,
    "TCRE065": 5,
    "TCRE068": 5,
    "TCRE069": 5,
    "TCRE070": 5,
    "TCRE071": 5,
    "TCRE072": 5,
    "TCRE073": 5,
    "TCRE084": 5,
    "TCRE085": 5,
    "TCRE086": 5,
    "TCRE087": 5,
    "TCRE088": 5,
    "TCRE089": 5,
    "TCRE092": 5,
    "TCRE093": 5,
    "TCRE094": 5,
    "TCRE095": 5,
    "TCRE096": 5,
    "TCRE097": 5,
    "TCRE100": 5,
    "TCRE108": 5,
    "TCMG011": 5,
    "TCMG014": 5,
    "TCMG015": 5,
    "TCMG016": 5,
    "TCMG020": 5,
    "TCMG022": 5,
    "TCMG023": 5,
    "TCMG029": 5,
    "TCMG030": 5,
    "TCMG031": 9,
    "TCMG034": 9,
    "TCMG035": 9,
    "TCMG036": 6,
    "TCMG055": 5,
    "TCMG060": 5,
    "TCMG061": 5,
    "TCMG062": 5,
    "TCMG063": 5,
    "TCMG064": 5,
    "TCMG065": 5,
    "TCMG068": 5,
    "TCMG069": 5,
    "TCMG070": 5,
    "TCMG071": 5,
    "TCMG072": 5,
    "TCMG073": 5,
    "TCMG084": 5,
    "TCMG085": 5,
    "TCMG086": 5,
    "TCMG087": 5,
    "TCMG088": 5,
    "TCMG089": 5,
    "TCMG092": 5,
    "TCMG093": 5,
    "TCMG094": 5,
    "TCMG095": 5,
    "TCMG096": 5,
    "TCMG097": 5,
    "TCMG100": 5,
    "TCMG108": 5,
    "TCBK011": 5,
    "TCBK014": 5,
    "TCBK015": 5,
    "TCBK016": 5,
    "TCBK020": 5,
    "TCBK022": 5,
    "TCBK023": 5,
    "TCBK029": 5,
    "TCBK030": 5,
    "TCBK031": 9,
    "TCBK034": 9,
    "TCBK035": 9,
    "TCBK036": 6,
    "TCBK055": 5,
    "TCBK060": 5,
    "TCBK061": 5,
    "TCBK062": 5,
    "TCBK063": 5,
    "TCBK064": 5,
    "TCBK065": 5,
    "TCBK068": 5,
    "TCBK069": 5,
    "TCBK070": 5,
    "TCBK071": 5,
    "TCBK072": 5,
    "TCBK073": 5,
    "TCBK084": 5,
    "TCBK085": 5,
    "TCBK086": 5,
    "TCBK087": 5,
    "TCBK088": 5,
    "TCBK089": 5,
    "TCBK092": 5,
    "TCBK093": 5,
    "TCBK094": 5,
    "TCBK095": 5,
    "TCBK096": 5,
    "TCBK097": 5,
    "TCBK100": 5,
    "TCBK108": 5,
    "TCAI011": 5,
    "TCAI014": 5,
    "TCAI015": 5,
    "TCAI016": 5,
    "TCAI020": 5,
    "TCAI022": 5,
    "TCAI023": 5,
    "TCAI029": 5,
    "TCAI030": 5,
    "TCAI031": 9,
    "TCAI034": 9,
    "TCAI035": 9,
    "TCAI036": 6,
    "TCAI055": 5,
    "TCAI060": 5,
    "TCAI061": 5,
    "TCAI062": 5,
    "TCAI063": 5,
    "TCAI064": 5,
    "TCAI065": 5,
    "TCAI068": 5,
    "TCAI069": 5,
    "TCAI070": 5,
    "TCAI071": 5,
    "TCAI072": 5,
    "TCAI073": 5,
    "TCAI084": 5,
    "TCAI085": 5,
    "TCAI086": 5,
    "TCAI087": 5,
    "TCAI088": 5,
    "TCAI089": 5,
    "TCAI092": 5,
    "TCAI093": 5,
    "TCAI094": 5,
    "TCAI095": 5,
    "TCAI096": 5,
    "TCAI097": 5,
    "TCAI100": 5,
    "TCAI108": 5,
    "TCAR011": 5,
    "TCAR014": 5,
    "TCAR015": 5,
    "TCAR016": 5,
    "TCAR020": 5,
    "TCAR022": 5,
    "TCAR023": 5,
    "TCAR029": 5,
    "TCAR030": 5,
    "TCAR031": 9,
    "TCAR034": 9,
    "TCAR035": 9,
    "TCAR036": 6,
    "TCAR055": 5,
    "TCAR060": 5,
    "TCAR061": 5,
    "TCAR062": 5,
    "TCAR063": 5,
    "TCAR064": 5,
    "TCAR065": 5,
    "TCAR068": 5,
    "TCAR069": 5,
    "TCAR070": 5,
    "TCAR071": 5,
    "TCAR072": 5,
    "TCAR073": 5,
    "TCAR084": 5,
    "TCAR085": 5,
    "TCAR086": 5,
    "TCAR087": 5,
    "TCAR088": 5,
    "TCAR089": 5,
    "TCAR092": 5,
    "TCAR093": 5,
    "TCAR094": 5,
    "TCAR095": 5,
    "TCAR096": 5,
    "TCAR097": 5,
    "TCAR100": 5,
    "TCAR108": 5,
    "TCBI011": 5,
    "TCBI014": 5,
    "TCBI015": 5,
    "TCBI016": 5,
    "TCBI020": 5,
    "TCBI022": 5,
    "TCBI023": 5,
    "TCBI029": 5,
    "TCBI030": 5,
    "TCBI031": 9,
    "TCBI034": 9,
    "TCBI035": 9,
    "TCBI036": 6,
    "TCBI055": 5,
    "TCBI060": 5,
    "TCBI061": 5,
    "TCBI062": 5,
    "TCBI063": 5,
    "TCBI064": 5,
    "TCBI065": 5,
    "TCBI068": 5,
    "TCBI069": 5,
    "TCBI070": 5,
    "TCBI071": 5,
    "TCBI072": 5,
    "TCBI073": 5,
    "TCBI084": 5,
    "TCBI085": 5,
    "TCBI086": 5,
    "TCBI087": 5,
    "TCBI088": 5,
    "TCBI089": 5,
    "TCBI092": 5,
    "TCBI093": 5,
    "TCBI094": 5,
    "TCBI095": 5,
    "TCBI096": 5,
    "TCBI097": 5,
    "TCBI100": 5,
    "TCBI108": 5,
    "TCBR011": 5,
    "TCBR014": 5,
    "TCBR015": 5,
    "TCBR016": 5,
    "TCBR020": 5,
    "TCBR022": 5,
    "TCBR023": 5,
    "TCBR029": 5,
    "TCBR030": 5,
    "TCBR031": 9,
    "TCBR034": 9,
    "TCBR035": 9,
    "TCBR036": 6,
    "TCBR055": 5,
    "TCBR060": 5,
    "TCBR061": 5,
    "TCBR062": 5,
    "TCBR063": 5,
    "TCBR064": 5,
    "TCBR065": 5,
    "TCBR068": 5,
    "TCBR069": 5,
    "TCBR070": 5,
    "TCBR071": 5,
    "TCBR072": 5,
    "TCBR073": 5,
    "TCBR084": 5,
    "TCBR085": 5,
    "TCBR086": 5,
    "TCBR087": 5,
    "TCBR088": 5,
    "TCBR089": 5,
    "TCBR092": 5,
    "TCBR093": 5,
    "TCBR094": 5,
    "TCBR095": 5,
    "TCBR096": 5,
    "TCBR097": 5,
    "TCBR100": 5,
    "TCBR108": 5,
    "TCBL011": 5,
    "TCBL014": 5,
    "TCBL015": 5,
    "TCBL016": 5,
    "TCBL020": 5,
    "TCBL022": 5,
    "TCBL023": 5,
    "TCBL029": 5,
    "TCBL030": 5,
    "TCBL031": 9,
    "TCBL034": 9,
    "TCBL035": 9,
    "TCBL036": 6,
    "TCBL055": 5,
    "TCBL060": 5,
    "TCBL061": 5,
    "TCBL062": 5,
    "TCBL063": 5,
    "TCBL064": 5,
    "TCBL065": 5,
    "TCBL068": 5,
    "TCBL069": 5,
    "TCBL070": 5,
    "TCBL071": 5,
    "TCBL072": 5,
    "TCBL073": 5,
    "TCBL084": 5,
    "TCBL085": 5,
    "TCBL086": 5,
    "TCBL087": 5,
    "TCBL088": 5,
    "TCBL089": 5,
    "TCBL092": 5,
    "TCBL093": 5,
    "TCBL094": 5,
    "TCBL095": 5,
    "TCBL096": 5,
    "TCBL097": 5,
    "TCBL100": 5,
    "TCBL108": 5,
    "TCCU011": 5,
    "TCCU014": 5,
    "TCCU015": 5,
    "TCCU016": 5,
    "TCCU020": 5,
    "TCCU022": 5,
    "TCCU023": 5,
    "TCCU029": 5,
    "TCCU030": 5,
    "TCCU031": 9,
    "TCCU034": 9,
    "TCCU035": 9,
    "TCCU036": 6,
    "TCCU055": 5,
    "TCCU060": 5,
    "TCCU061": 5,
    "TCCU062": 5,
    "TCCU063": 5,
    "TCCU064": 5,
    "TCCU065": 5,
    "TCCU068": 5,
    "TCCU069": 5,
    "TCCU070": 5,
    "TCCU071": 5,
    "TCCU072": 5,
    "TCCU073": 5,
    "TCCU084": 5,
    "TCCU085": 5,
    "TCCU086": 5,
    "TCCU087": 5,
    "TCCU088": 5,
    "TCCU089": 5,
    "TCCU092": 5,
    "TCCU093": 5,
    "TCCU094": 5,
    "TCCU095": 5,
    "TCCU096": 5,
    "TCCU097": 5,
    "TCCU100": 5,
    "TCCU108": 5,
    "TCCI011": 5,
    "TCCI014": 5,
    "TCCI015": 5,
    "TCCI016": 5,
    "TCCI020": 5,
    "TCCI022": 5,
    "TCCI023": 5,
    "TCCI029": 5,
    "TCCI030": 5,
    "TCCI031": 9,
    "TCCI034": 9,
    "TCCI035": 9,
    "TCCI036": 6,
    "TCCI055": 5,
    "TCCI060": 5,
    "TCCI061": 5,
    "TCCI062": 5,
    "TCCI063": 5,
    "TCCI064": 5,
    "TCCI065": 5,
    "TCCI068": 5,
    "TCCI069": 5,
    "TCCI070": 5,
    "TCCI071": 5,
    "TCCI072": 5,
    "TCCI073": 5,
    "TCCI084": 5,
    "TCCI085": 5,
    "TCCI086": 5,
    "TCCI087": 5,
    "TCCI088": 5,
    "TCCI089": 5,
    "TCCI092": 5,
    "TCCI093": 5,
    "TCCI094": 5,
    "TCCI095": 5,
    "TCCI096": 5,
    "TCCI097": 5,
    "TCCI100": 5,
    "TCCI108": 5,
    "TCCR011": 5,
    "TCCR014": 5,
    "TCCR015": 5,
    "TCCR016": 5,
    "TCCR020": 5,
    "TCCR022": 5,
    "TCCR023": 5,
    "TCCR029": 5,
    "TCCR030": 5,
    "TCCR031": 9,
    "TCCR034": 9,
    "TCCR035": 9,
    "TCCR036": 6,
    "TCCR055": 5,
    "TCCR060": 5,
    "TCCR061": 5,
    "TCCR062": 5,
    "TCCR063": 5,
    "TCCR064": 5,
    "TCCR065": 5,
    "TCCR068": 5,
    "TCCR069": 5,
    "TCCR070": 5,
    "TCCR071": 5,
    "TCCR072": 5,
    "TCCR073": 5,
    "TCCR084": 5,
    "TCCR085": 5,
    "TCCR086": 5,
    "TCCR087": 5,
    "TCCR088": 5,
    "TCCR089": 5,
    "TCCR092": 5,
    "TCCR093": 5,
    "TCCR094": 5,
    "TCCR095": 5,
    "TCCR096": 5,
    "TCCR097": 5,
    "TCCR100": 5,
    "TCCR108": 5,
    "TCHE011": 5,
    "TCHE014": 5,
    "TCHE015": 5,
    "TCHE016": 5,
    "TCHE020": 5,
    "TCHE022": 5,
    "TCHE023": 5,
    "TCHE029": 5,
    "TCHE030": 5,
    "TCHE031": 9,
    "TCHE034": 9,
    "TCHE035": 9,
    "TCHE036": 6,
    "TCHE055": 5,
    "TCHE060": 5,
    "TCHE061": 5,
    "TCHE062": 5,
    "TCHE063": 5,
    "TCHE064": 5,
    "TCHE065": 5,
    "TCHE068": 5,
    "TCHE069": 5,
    "TCHE070": 5,
    "TCHE071": 5,
    "TCHE072": 5,
    "TCHE073": 5,
    "TCHE084": 5,
    "TCHE085": 5,
    "TCHE086": 5,
    "TCHE087": 5,
    "TCHE088": 5,
    "TCHE089": 5,
    "TCHE092": 5,
    "TCHE093": 5,
    "TCHE094": 5,
    "TCHE095": 5,
    "TCHE096": 5,
    "TCHE097": 5,
    "TCHE100": 5,
    "TCHE108": 5,
    "TCMO011": 5,
    "TCMO014": 5,
    "TCMO015": 5,
    "TCMO016": 5,
    "TCMO020": 5,
    "TCMO022": 5,
    "TCMO023": 5,
    "TCMO029": 5,
    "TCMO030": 5,
    "TCMO031": 9,
    "TCMO034": 9,
    "TCMO035": 9,
    "TCMO036": 6,
    "TCMO055": 5,
    "TCMO060": 5,
    "TCMO061": 5,
    "TCMO062": 5,
    "TCMO063": 5,
    "TCMO064": 5,
    "TCMO065": 5,
    "TCMO068": 5,
    "TCMO069": 5,
    "TCMO070": 5,
    "TCMO071": 5,
    "TCMO072": 5,
    "TCMO073": 5,
    "TCMO084": 5,
    "TCMO085": 5,
    "TCMO086": 5,
    "TCMO087": 5,
    "TCMO088": 5,
    "TCMO089": 5,
    "TCMO092": 5,
    "TCMO093": 5,
    "TCMO094": 5,
    "TCMO095": 5,
    "TCMO096": 5,
    "TCMO097": 5,
    "TCMO100": 5,
    "TCMO108": 5,
    "TCNC011": 5,
    "TCNC014": 5,
    "TCNC015": 5,
    "TCNC016": 5,
    "TCNC020": 5,
    "TCNC022": 5,
    "TCNC023": 5,
    "TCNC029": 5,
    "TCNC030": 5,
    "TCNC031": 9,
    "TCNC034": 9,
    "TCNC035": 9,
    "TCNC036": 6,
    "TCNC055": 5,
    "TCNC060": 5,
    "TCNC061": 5,
    "TCNC062": 5,
    "TCNC063": 5,
    "TCNC064": 5,
    "TCNC065": 5,
    "TCNC068": 5,
    "TCNC069": 5,
    "TCNC070": 5,
    "TCNC071": 5,
    "TCNC072": 5,
    "TCNC073": 5,
    "TCNC084": 5,
    "TCNC085": 5,
    "TCNC086": 5,
    "TCNC087": 5,
    "TCNC088": 5,
    "TCNC089": 5,
    "TCNC092": 5,
    "TCNC093": 5,
    "TCNC094": 5,
    "TCNC095": 5,
    "TCNC096": 5,
    "TCNC097": 5,
    "TCNC100": 5,
    "TCNC108": 5,
    "TCAF011": 5,
    "TCAF014": 5,
    "TCAF015": 5,
    "TCAF016": 5,
    "TCAF020": 5,
    "TCAF022": 5,
    "TCAF023": 5,
    "TCAF029": 5,
    "TCAF030": 5,
    "TCAF031": 9,
    "TCAF034": 9,
    "TCAF035": 9,
    "TCAF036": 6,
    "TCAF055": 5,
    "TCAF060": 5,
    "TCAF061": 5,
    "TCAF062": 5,
    "TCAF063": 5,
    "TCAF064": 5,
    "TCAF065": 5,
    "TCAF068": 5,
    "TCAF069": 5,
    "TCAF070": 5,
    "TCAF071": 5,
    "TCAF072": 5,
    "TCAF073": 5,
    "TCAF084": 5,
    "TCAF085": 5,
    "TCAF086": 5,
    "TCAF087": 5,
    "TCAF088": 5,
    "TCAF089": 5,
    "TCAF092": 5,
    "TCAF093": 5,
    "TCAF094": 5,
    "TCAF095": 5,
    "TCAF096": 5,
    "TCAF097": 5,
    "TCAF100": 5,
    "TCAF108": 5,
    "TCPF011": 5,
    "TCPF014": 5,
    "TCPF015": 5,
    "TCPF016": 5,
    "TCPF020": 5,
    "TCPF022": 5,
    "TCPF023": 5,
    "TCPF029": 5,
    "TCPF030": 5,
    "TCPF031": 9,
    "TCPF034": 9,
    "TCPF035": 9,
    "TCPF036": 6,
    "TCPF055": 5,
    "TCPF060": 5,
    "TCPF061": 5,
    "TCPF062": 5,
    "TCPF063": 5,
    "TCPF064": 5,
    "TCPF065": 5,
    "TCPF068": 5,
    "TCPF069": 5,
    "TCPF070": 5,
    "TCPF071": 5,
    "TCPF072": 5,
    "TCPF073": 5,
    "TCPF084": 5,
    "TCPF085": 5,
    "TCPF086": 5,
    "TCPF087": 5,
    "TCPF088": 5,
    "TCPF089": 5,
    "TCPF092": 5,
    "TCPF093": 5,
    "TCPF094": 5,
    "TCPF095": 5,
    "TCPF096": 5,
    "TCPF097": 5,
    "TCPF100": 5,
    "TCPF108": 5,
    "TCPI011": 5,
    "TCPI014": 5,
    "TCPI015": 5,
    "TCPI016": 5,
    "TCPI020": 5,
    "TCPI022": 5,
    "TCPI023": 5,
    "TCPI029": 5,
    "TCPI030": 5,
    "TCPI031": 9,
    "TCPI034": 9,
    "TCPI035": 9,
    "TCPI036": 6,
    "TCPI055": 5,
    "TCPI060": 5,
    "TCPI061": 5,
    "TCPI062": 5,
    "TCPI063": 5,
    "TCPI064": 5,
    "TCPI065": 5,
    "TCPI068": 5,
    "TCPI069": 5,
    "TCPI070": 5,
    "TCPI071": 5,
    "TCPI072": 5,
    "TCPI073": 5,
    "TCPI084": 5,
    "TCPI085": 5,
    "TCPI086": 5,
    "TCPI087": 5,
    "TCPI088": 5,
    "TCPI089": 5,
    "TCPI092": 5,
    "TCPI093": 5,
    "TCPI094": 5,
    "TCPI095": 5,
    "TCPI096": 5,
    "TCPI097": 5,
    "TCPI100": 5,
    "TCPI108": 5,
    "TCPR011": 5,
    "TCPR014": 5,
    "TCPR015": 5,
    "TCPR016": 5,
    "TCPR020": 5,
    "TCPR022": 5,
    "TCPR023": 5,
    "TCPR029": 5,
    "TCPR030": 5,
    "TCPR031": 9,
    "TCPR034": 9,
    "TCPR035": 9,
    "TCPR036": 6,
    "TCPR055": 5,
    "TCPR060": 5,
    "TCPR061": 5,
    "TCPR062": 5,
    "TCPR063": 5,
    "TCPR064": 5,
    "TCPR065": 5,
    "TCPR068": 5,
    "TCPR069": 5,
    "TCPR070": 5,
    "TCPR071": 5,
    "TCPR072": 5,
    "TCPR073": 5,
    "TCPR084": 5,
    "TCPR085": 5,
    "TCPR086": 5,
    "TCPR087": 5,
    "TCPR088": 5,
    "TCPR089": 5,
    "TCPR092": 5,
    "TCPR093": 5,
    "TCPR094": 5,
    "TCPR095": 5,
    "TCPR096": 5,
    "TCPR097": 5,
    "TCPR100": 5,
    "TCPR108": 5,
    "TCSF011": 5,
    "TCSF014": 5,
    "TCSF015": 5,
    "TCSF016": 5,
    "TCSF020": 5,
    "TCSF022": 5,
    "TCSF023": 5,
    "TCSF029": 5,
    "TCSF030": 5,
    "TCSF031": 9,
    "TCSF034": 9,
    "TCSF035": 9,
    "TCSF036": 6,
    "TCSF055": 5,
    "TCSF060": 5,
    "TCSF061": 5,
    "TCSF062": 5,
    "TCSF063": 5,
    "TCSF064": 5,
    "TCSF065": 5,
    "TCSF068": 5,
    "TCSF069": 5,
    "TCSF070": 5,
    "TCSF071": 5,
    "TCSF072": 5,
    "TCSF073": 5,
    "TCSF084": 5,
    "TCSF085": 5,
    "TCSF086": 5,
    "TCSF087": 5,
    "TCSF088": 5,
    "TCSF089": 5,
    "TCSF092": 5,
    "TCSF093": 5,
    "TCSF094": 5,
    "TCSF095": 5,
    "TCSF096": 5,
    "TCSF097": 5,
    "TCSF100": 5,
    "TCSF108": 5,
    "TCSI011": 5,
    "TCSI014": 5,
    "TCSI015": 5,
    "TCSI016": 5,
    "TCSI020": 5,
    "TCSI022": 5,
    "TCSI023": 5,
    "TCSI029": 5,
    "TCSI030": 5,
    "TCSI031": 9,
    "TCSI034": 9,
    "TCSI035": 9,
    "TCSI036": 6,
    "TCSI055": 5,
    "TCSI060": 5,
    "TCSI061": 5,
    "TCSI062": 5,
    "TCSI063": 5,
    "TCSI064": 5,
    "TCSI065": 5,
    "TCSI068": 5,
    "TCSI069": 5,
    "TCSI070": 5,
    "TCSI071": 5,
    "TCSI072": 5,
    "TCSI073": 5,
    "TCSI084": 5,
    "TCSI085": 5,
    "TCSI086": 5,
    "TCSI087": 5,
    "TCSI088": 5,
    "TCSI089": 5,
    "TCSI092": 5,
    "TCSI093": 5,
    "TCSI094": 5,
    "TCSI095": 5,
    "TCSI096": 5,
    "TCSI097": 5,
    "TCSI100": 5,
    "TCSI108": 5,
    "TCSR011": 5,
    "TCSR014": 5,
    "TCSR015": 5,
    "TCSR016": 5,
    "TCSR020": 5,
    "TCSR022": 5,
    "TCSR023": 5,
    "TCSR029": 5,
    "TCSR030": 5,
    "TCSR031": 9,
    "TCSR034": 9,
    "TCSR035": 9,
    "TCSR036": 6,
    "TCSR055": 5,
    "TCSR060": 5,
    "TCSR061": 5,
    "TCSR062": 5,
    "TCSR063": 5,
    "TCSR064": 5,
    "TCSR065": 5,
    "TCSR068": 5,
    "TCSR069": 5,
    "TCSR070": 5,
    "TCSR071": 5,
    "TCSR072": 5,
    "TCSR073": 5,
    "TCSR084": 5,
    "TCSR085": 5,
    "TCSR086": 5,
    "TCSR087": 5,
    "TCSR088": 5,
    "TCSR089": 5,
    "TCSR092": 5,
    "TCSR093": 5,
    "TCSR094": 5,
    "TCSR095": 5,
    "TCSR096": 5,
    "TCSR097": 5,
    "TCSR100": 5,
    "TCSR108": 5,
    "TCRT011": 5,
    "TCRT014": 5,
    "TCRT015": 5,
    "TCRT016": 5,
    "TCRT020": 5,
    "TCRT022": 5,
    "TCRT023": 5,
    "TCRT029": 5,
    "TCRT030": 5,
    "TCRT031": 9,
    "TCRT034": 9,
    "TCRT035": 9,
    "TCRT036": 6,
    "TCRT055": 5,
    "TCRT060": 5,
    "TCRT061": 5,
    "TCRT062": 5,
    "TCRT063": 5,
    "TCRT064": 5,
    "TCRT065": 5,
    "TCRT068": 5,
    "TCRT069": 5,
    "TCRT070": 5,
    "TCRT071": 5,
    "TCRT072": 5,
    "TCRT073": 5,
    "TCRT084": 5,
    "TCRT085": 5,
    "TCRT086": 5,
    "TCRT087": 5,
    "TCRT088": 5,
    "TCRT089": 5,
    "TCRT092": 5,
    "TCRT093": 5,
    "TCRT094": 5,
    "TCRT095": 5,
    "TCRT096": 5,
    "TCRT097": 5,
    "TCRT100": 5,
    "TCRT108": 5,
    "TCRD011": 5,
    "TCRD014": 5,
    "TCRD015": 5,
    "TCRD016": 5,
    "TCRD020": 5,
    "TCRD022": 5,
    "TCRD023": 5,
    "TCRD029": 5,
    "TCRD030": 5,
    "TCRD031": 9,
    "TCRD034": 9,
    "TCRD035": 9,
    "TCRD036": 6,
    "TCRD055": 5,
    "TCRD060": 5,
    "TCRD061": 5,
    "TCRD062": 5,
    "TCRD063": 5,
    "TCRD064": 5,
    "TCRD065": 5,
    "TCRD068": 5,
    "TCRD069": 5,
    "TCRD070": 5,
    "TCRD071": 5,
    "TCRD072": 5,
    "TCRD073": 5,
    "TCRD084": 5,
    "TCRD085": 5,
    "TCRD086": 5,
    "TCRD087": 5,
    "TCRD088": 5,
    "TCRD089": 5,
    "TCRD092": 5,
    "TCRD093": 5,
    "TCRD094": 5,
    "TCRD095": 5,
    "TCRD096": 5,
    "TCRD097": 5,
    "TCRD100": 5,
    "TCRD108": 5,
    "TCSL011": 5,
    "TCSL014": 5,
    "TCSL015": 5,
    "TCSL016": 5,
    "TCSL020": 5,
    "TCSL022": 5,
    "TCSL023": 5,
    "TCSL029": 5,
    "TCSL030": 5,
    "TCSL031": 9,
    "TCSL034": 9,
    "TCSL035": 9,
    "TCSL036": 6,
    "TCSL055": 5,
    "TCSL060": 5,
    "TCSL061": 5,
    "TCSL062": 5,
    "TCSL063": 5,
    "TCSL064": 5,
    "TCSL065": 5,
    "TCSL068": 5,
    "TCSL069": 5,
    "TCSL070": 5,
    "TCSL071": 5,
    "TCSL072": 5,
    "TCSL073": 5,
    "TCSL084": 5,
    "TCSL085": 5,
    "TCSL086": 5,
    "TCSL087": 5,
    "TCSL088": 5,
    "TCSL089": 5,
    "TCSL092": 5,
    "TCSL093": 5,
    "TCSL094": 5,
    "TCSL095": 5,
    "TCSL096": 5,
    "TCSL097": 5,
    "TCSL100": 5,
    "TCSL108": 5,
    "TCTE011": 5,
    "TCTE014": 5,
    "TCTE015": 5,
    "TCTE016": 5,
    "TCTE020": 5,
    "TCTE022": 5,
    "TCTE023": 5,
    "TCTE029": 5,
    "TCTE030": 5,
    "TCTE031": 9,
    "TCTE034": 9,
    "TCTE035": 9,
    "TCTE036": 6,
    "TCTE055": 5,
    "TCTE060": 5,
    "TCTE061": 5,
    "TCTE062": 5,
    "TCTE063": 5,
    "TCTE064": 5,
    "TCTE065": 5,
    "TCTE068": 5,
    "TCTE069": 5,
    "TCTE070": 5,
    "TCTE071": 5,
    "TCTE072": 5,
    "TCTE073": 5,
    "TCTE084": 5,
    "TCTE085": 5,
    "TCTE086": 5,
    "TCTE087": 5,
    "TCTE088": 5,
    "TCTE089": 5,
    "TCTE092": 5,
    "TCTE093": 5,
    "TCTE094": 5,
    "TCTE095": 5,
    "TCTE096": 5,
    "TCTE097": 5,
    "TCTE100": 5,
    "TCTE108": 5,
    "TCTT011": 5,
    "TCTT014": 5,
    "TCTT015": 5,
    "TCTT016": 5,
    "TCTT020": 5,
    "TCTT022": 5,
    "TCTT023": 5,
    "TCTT029": 5,
    "TCTT030": 5,
    "TCTT031": 9,
    "TCTT034": 9,
    "TCTT035": 9,
    "TCTT036": 6,
    "TCTT055": 5,
    "TCTT060": 5,
    "TCTT061": 5,
    "TCTT062": 5,
    "TCTT063": 5,
    "TCTT064": 5,
    "TCTT065": 5,
    "TCTT068": 5,
    "TCTT069": 5,
    "TCTT070": 5,
    "TCTT071": 5,
    "TCTT072": 5,
    "TCTT073": 5,
    "TCTT084": 5,
    "TCTT085": 5,
    "TCTT086": 5,
    "TCTT087": 5,
    "TCTT088": 5,
    "TCTT089": 5,
    "TCTT092": 5,
    "TCTT093": 5,
    "TCTT094": 5,
    "TCTT095": 5,
    "TCTT096": 5,
    "TCTT097": 5,
    "TCTT100": 5,
    "TCTT108": 5,
}

default_args = {
    "owner": "tc",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 1, 7),
    "retries": 0,
}

dag = DAG(
    "equifax_consumer_batch_output_dag", schedule_interval=None, default_args=default_args
)

def get_month_tag(date):
    last_month = date.month - 1 if date.month > 1 else 12
    last_year = date.year - 1 if last_month == 12 else date.year
    return f"{last_year}{str(last_month).zfill(2)}"

snowflake_hook = BaseHook.get_connection("snowflake_test")
#aws_hook = AwsHook(aws_conn_id="aws_default")
#aws_credentials = aws_hook.get_credentials()
aws_credentials = type('aws_credentials', (object,), {'access_key' : '123', 'secret_key' : '123'})
now = datetime.now()
month_tag = get_month_tag(now)
path = f"equifax_offline_batch/consumer/output/tc_consumer_batch_{month_tag}.txt"

def convert_line(line):
    indices = _gen_arr(0, result_dict_1) + _gen_arr(94, result_dict_2)
    #print(indices)
    parts = ['"{}"'.format(line[i:j].strip()) for i, j in zip(indices, indices[1:] + [None])]
    #_gen_header(**result_dict_1, **result_dict_2)
    return '|'.join(parts)

def _gen_arr(start, dol):
    result = [start]
    for l in dol:
        result.append(result[-1] + dol[l])
    return result[:-1]

# def _gen_header(**kwargs):
# 	header = []
# 	for k in kwargs:
# 		header.append('"{}"'.format(k))
# 	#print(','.join(header))
# 	return ','.join(header)

def get_col_def(n, l):
    return f"{n} varchar({l})"

def convert_date_format(value):
    if "-" not in value and not value.isspace():
        try:
            m = value[:2]
            d = value[2:4]
            y = value[4:]
            if int(y) < 20:
                yy = f"20{y}"
            else:
                yy = f"19{y}"
            d = datetime.strptime(f"{yy}-{m}-{d}", "%Y-%m-%d")
            return d
        except Exception as e:
            print(e)
    return None

def get_client():
    return boto3.client(
        "s3",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    )

def get_file():
    objects = get_client().list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
    for content in objects["Contents"]:
        if content["Key"].endswith("out1"):
            return content["Key"]
    return None

def read_output():
    client = get_client()
    file_path = get_file()
    print(f"Reading file: {file_path}")
    obj = client.get_object(Bucket=bucket, Key=f"{file_path}")
    body = obj["Body"].read()
    contents = body.decode("ISO-8859-1")
    with tempfile.TemporaryFile(mode="w+") as raw, tempfile.NamedTemporaryFile(mode="w+") as formatted:
        raw.write(contents)
        raw.seek(0)
        d3 = {**result_dict_1, **result_dict_2}
        lines = []
        for line in raw.readlines():
            if not line.startswith("BHDR-EQUIFAX") and not line.startswith(
                "BTRL-EQUIFAX"
            ):
                lines.append(convert_line(line))
        formatted.writelines(lines)
        formatted.seek(0)
        with open(formatted.name, mode="rb") as f:
            get_client().upload_fileobj(f, bucket, path)
            print(f"File uploaded:{path}")

def create_table(snowflake_conn: str):
    d3 = {**result_dict_1, **result_dict_2}
    print(f"Size of dict: {len(d3)}")
    cols = []
    raw_table_name = "equifax.output.consumer_batch_raw"
    for col in d3:
        line = get_col_def(col, d3[col])
        cols.append(line)
    #print(cols)
    create = f"create or replace table {raw_table_name} ({','.join(cols)});"
    copy = f"""COPY INTO {raw_table_name} FROM S3://{bucket}/{path} CREDENTIALS = (
                                            aws_key_id='{aws_credentials.access_key}',
                                            aws_secret_key='{aws_credentials.secret_key}')
                                            FILE_FORMAT=(field_delimiter='|')
                                            """
    # with snowflake.connector.connect(
    #     user=snowflake_hook.login,
    #     password=snowflake_hook.password,
    #     account=snowflake_hook.host,
    #     warehouse="ETL",
    #     database="EQUIFAX",
    #     schema="TEST",
    #     ocsp_fail_open=False,
    # ) as conn:
    #     cur = conn.cursor()
    #     cur.execute(create)
    #     cur.execute(copy)
    #     conn.commit()

    kwargs = {
        'connect_args': {"authenticator": "externalbrowser"}
    }
    raw_table_name_test = 'SANDBOX.JMASSON.CONSUMER_BATCH_RAW'
    #print(','.join(cols))
    cols = []
    value_cols = []
    for col in d3:
        #print(col)
        cols.append(get_col_def(col, d3[col]))
        value_cols.append(col)

    with open('/home/jmr/airflow/dags/tc_consumer_batch_202006.txt', 'r', encoding='windows-1252') as f: 
        with open('/home/jmr/airflow/dags/tc_consumer_batch_202006utf8.txt', 'w', encoding='utf-8') as f_utf8:
            f_utf8.write(f.read())
            f_utf8.close()


    #f = open('/home/jmr/airflow/dags/tc_consumer_batch_202006.txt', 'r', encoding='windows-1252')
    #target = open('/home/jmr/airflow/dags/tc_consumer_batch_202006utf8.txt', 'w')
    #target.write(unicode(f.read()))
    #values = []
    #for i in f.readlines():
    #    print(i)
        #value = []
        #for s in i.split('|'):
        #    #print(s.lstrip().rstrip())
        #    value.append(f"'{s.lstrip().rstrip()}'")
        #values.append(f'{",".join(value)}')
        #print(f'({",".join(value)})')
    #print(cols)
    # sql = f"CREATE OR REPLACE TABLE {raw_table_name_test} ({','.join(cols)});"
    # with SnowflakeHook(snowflake_conn).get_sqlalchemy_engine(kwargs).begin() as sfh:
    #     result = sfh.execute(sql)
    #     f = open('/home/jmr/airflow/dags/consumer_og3.csv', 'r', encoding='windows-1252')
    #     values = []
    #     for i in f.readlines():
    #         print(i)
    #         value = []
    #         for s in i.split('|'):
    #             #print(s.lstrip().rstrip())
    #             value.append(f"'{s.lstrip().rstrip()}'")
    #         values.append(f'{",".join(value)}')
    #         print(f'({",".join(value)})')
        #print(f'{values}'.strip('[]'))
        #print(values)
        #print(f"{f.readlines()}")
        #print(f.read())
        
        #copy = f"""COPY INTO {raw_table_name_test} FROM {f.read()}
        #                                    FILE_FORMAT=(field_delimiter='|')
        #                                    """
        #print(','.join(values))
        #insert = f"INSERT INTO SANDBOX.JMASSON.CONSUMER_BATCH_RAW ({','.join(value_cols)}) VALUES({','.join(values)});"
        #result = sfh.execute(insert)
        #print(result)

def fix_date_format():
    select = "select * from equifax.output.consumer_batch_raw"
    with snowflake.connector.connect(
        user=snowflake_hook.login,
        password=snowflake_hook.password,
        account=snowflake_hook.host,
        warehouse="ETL",
        database="EQUIFAX",
        schema="TEST",
        ocsp_fail_open=False,
    ) as conn:
        cur = conn.cursor()
        cur.execute(select)
        data = cur.fetchall()
        df = pd.DataFrame(data)
        df.columns = [des[0] for des in cur.description]
        print("Converting columns...")
        for key in [
            "DOB_TEXT",
            "PRXX014",
            "PRXX016",
            "PRXX039",
            "PRXX044",
            "INQAL009",
            "INQAM009",
            "INQMG009",
            "INQBK009",
            "INQCU009",
            "INQNC009",
            "INQAF009",
            "INQPF009",
            "INQSF009",
            "INQRT009",
            "INQRD009",
            "INQTE009",
            "INQBD009",
            "INQCL009",
        ]:
            df[key] = df[key].apply(lambda x: convert_date_format(x))
        print("Writing to csv...")
        with tempfile.NamedTemporaryFile(mode="w") as file_in:
            df.to_csv(file_in.name, index=False, sep="\t")
            with open(file_in.name, "rb") as file:
                print(f"Uploading converted csv to S3: {path}")
                get_client().upload_fileobj(file, bucket, path)
        print("start copying from s3")
        table_name = "equifax.output.consumer_batch"
        cur.execute(f"truncate table {table_name}")
        cur.execute(
            f"""COPY INTO {table_name} FROM S3://{bucket}/{path} CREDENTIALS = (
                                aws_key_id='{aws_credentials.access_key}',
                                aws_secret_key='{aws_credentials.secret_key}')
                                FILE_FORMAT=(field_delimiter='\t', FIELD_OPTIONALLY_ENCLOSED_BY = '"', skip_header=1, NULL_IF = ('     ','      ','         '));"""
        )

def merge_ref():
    sql = f"""
            insert into equifax.public.consumer_batch
            select '{month_tag}' as import_month,
                    u.account_id as accountid,
                    u.contract_id as contractid,
                    u.company_name as business_name,
                    c.*
            from equifax.input_history.consumer_202004_20200430223010 u
                    join equifax.output.consumer_batch c
                    on u.customer_reference_number = c.customer_reference_number;
            """
    with snowflake.connector.connect(
        user=snowflake_hook.login,
        password=snowflake_hook.password,
        account=snowflake_hook.host,
        warehouse="ETL",
        database="EQUIFAX",
        schema="TEST",
        ocsp_fail_open=False,
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql)

def test_conn(snowflake_conn: str):
    sql = f'SELECT * FROM jmasson.consumer_batch_raw'
    kwargs = {
        'connect_args': {"authenticator": "externalbrowser"}
    }
    print(SnowflakeHook(snowflake_conn))
    print(SnowflakeHook(snowflake_conn).get_sqlalchemy_engine())

    #engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine(kwargs)
    #print(engine_.begin())
    #print(snowflake_conn)
    #print(engine_)
    with SnowflakeHook(snowflake_conn).get_sqlalchemy_engine(kwargs).begin() as s:
        rows = s.execute(f'SELECT * FROM jmasson.consumer_batch_raw')
        print(rows)
        for row in rows:
            print(row)


# def create_dag() -> DAG:
# 	with DAG(
# 		f"parse_consumer_batch_output",
# 		schedule_interval=None,
# 		catchup=False
# 	) as dag:
# 		dag << PythonOperator(
# 			task_id=f"parse",
# 			python_callable=read_output,
# 			op_kwargs={},
# 			retry_delay=datetime.timedelta(hours=1),
# 			retries=3,
# 			executor_config={"resources": {"request_memory": "8Gi"},},
# 		) >> PythonOperator(
# 			task_id=f"parse",
# 			python_callable=create_table,
# 			retry_delay=datetime.timedelta(hours=1),
# 			retries=3,
# 			executor_config={"resources": {"request_memory": "8Gi"},},
# 		) >> PythonOperator(

# 		)

# 		return dag

# task_read_output = PythonOperator(
#     task_id="read_output",
#     python_callable=read_output,
#     dag=dag
# )

task_create_table = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    op_kwargs={
        "snowflake_conn": "snowflake_test"
    },
    dag=dag
)

# task_fix_date_format = PythonOperator(
#     task_id="fix_date_format",
#     python_callable=fix_date_format,
#     dag=dag
# )

# task_merge_ref = PythonOperator(
#     task_id="merge_ref",
#     python_callable=merge_ref,
#     dag=dag
# )

task_test_conn = PythonOperator(
    task_id = 'test_conn',
    python_callable=test_conn,
    op_kwargs={
        "snowflake_conn": "snowflake_test"
    },
    dag=dag
)

task_test_conn

#task_read_output >> task_create_table >> task_fix_date_format >> task_merge_ref

if __name__ == "__main__":
    import os
    from unittest.mock import MagicMock, patch
    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "thinkingcapital.ca-central-1.aws")
    database = os.environ.get("SNOWFLAKE_DATABASE", "SANDBOX")
    role = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")

    url = (
        URL(account=account, database=database, role=role)
        if role
        else URL(account=account, database=database)
    )

    #mock = MagicMock()
    #mock.login = os.environ.get("SALESFORCE_USERNAME")
    #mock.password = os.environ.get("SALESFORCE_PASSWORD")
    #mock.extra_dejson = {"security_token": os.environ.get("SALESFORCE_TOKEN")}

    #with patch(
    #    "dags.parse_consumer_batch_output_dag.SnowflakeHook.get_sqlalchemy_engine",
    #    return_value=create_engine(
    #        url, connect_args={"authenticator": "externalbrowser",},
    #    ),
    #) as mock_engine:
    #test_conn('snowflake_test')
    create_table('snowflake_test')
        #import_sfdc("snowflake_conn", "salesforce_conn", "sfoi")
        #read_output()
        #create_table()
        #fix_date_format()
        #merge_ref()