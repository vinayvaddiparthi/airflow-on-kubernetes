import json
from datetime import datetime
import requests
import xmltodict
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from requests.auth import HTTPBasicAuth
from netsuite_extras import netsuite_helper

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 21),
    "retries": 0,
}

with DAG("erp_update_employees", schedule_interval=None, default_args=default_args) as dag:
    bamboohr_hook = BaseHook.get_connection("bamboohr")
    api_key = bamboohr_hook.password
    url_prefix = f"{bamboohr_hook.host}{bamboohr_hook.login}{bamboohr_hook.schema}"

    netsuite_hook = BaseHook.get_connection("netsuite_sandbox")
    email = netsuite_hook.login
    password = netsuite_hook.password
    account = netsuite_hook.schema
    app_id = json.loads(netsuite_hook.extra)["app_id"]
    endpoint = netsuite_hook.host


    def response_to_json(r):
        r_dict_str = json.dumps(xmltodict.parse(r.content))
        return json.loads(r_dict_str)


    def refresh_employees():
        print("Getting employees from BambooHR")
        r = requests.get(url_prefix, auth=HTTPBasicAuth(api_key, 'random_string'))
        with open('employees.xml', 'wb') as f:
            f.write(r.content)
        j = xmltodict.parse(r.content)
        employees = j['directory']['employees']
        employees_list = []
        for employee in employees['employee']:
            e = {'id': employee['@id']}
            for field in employee['field']:
                if '#text' in field:
                    e[field['@id']] = field['#text']
            employees_list.append(e)
        print(f"{len(employees_list)} of employees fetched from API")

        print("Searching for employees in Netsuite")
        headers = {'content-type': 'application/xml',
                   'soapaction': 'Search',
                   'cache-control': 'no-cache'}
        payload = netsuite_helper.search_employees('email', 'fake@value.ca', email, password, account, app_id)
        r_dict = response_to_json(requests.post(endpoint, headers=headers, data=payload))
        try:
            search_result = r_dict["soapenv:Envelope"]["soapenv:Body"]["searchResponse"]["platformCore:searchResult"]
            if "true" == search_result["platformCore:status"]["@isSuccess"]:
                count = int(search_result["platformCore:totalRecords"])
                print(f"Employee count in NS: {count}")
                record_list = search_result["platformCore:recordList"]["platformCore:record"]
                emails_ns = set([record["listEmp:email"] for record in record_list])
                emails_hr = set([employee["workEmail"] for employee in employees_list])

                emails_new = emails_hr - emails_ns
                emails_inactive = emails_ns - emails_hr
                emails_update = emails_hr & emails_ns
                new_employees = []
                inactive_employees = []
                update_employees = []
                for employee in employees_list:
                    if employee["workEmail"] in emails_new:
                        gender = employee["gender"].lower()
                        new_employees.append(netsuite_helper.employee(
                            employee["firstName"],
                            employee["lastName"],
                            f"_{gender}",
                            employee["jobTitle"],
                            None,
                            None,
                            6,
                            employee["workEmail"],
                        ))

                    if employee["workEmail"] in emails_update:
                        gender = employee["gender"].lower()
                        update_employees.append(netsuite_helper.employee(
                            employee["firstName"],
                            employee["lastName"],
                            f"_{gender}",
                            employee["jobTitle"],
                            None,
                            None,
                            6,
                            employee["workEmail"],
                        ))

                    # if employee["workEmail"] in emails_inactive:
                    #     inactive_employees.append(netsuite_helper.employee(
                    #         employee["firstName"],
                    #         employee["lastName"],
                    #         employee["gender"],
                    #         employee["jobTitle"],
                    #         None,
                    #         None,
                    #         6,
                    #         employee["workEmail"],
                    #     ))
                payload_add = netsuite_helper.get_body(netsuite_helper.add(new_employees),
                                                       email, password, account, app_id)
                payload_update = netsuite_helper.get_body(netsuite_helper.update(update_employees),
                                                          email, password, account, app_id)
                # payload_inactive = netsuite_helper.get_body(netsuite_helper.add(inactive_employees),
                #                                        email, password, account, app_id)
                print('\n' + payload_add + '\n')
                headers_add = {'content-type': 'application/xml',
                               'soapaction': 'Add' if len(update_employees) == 1 else 'AddeList',
                               'cache-control': 'no-cache'}
                r_add = response_to_json(requests.post(endpoint, headers=headers_add, data=payload_add))
                print(F"Added:\n{r_add}")
                headers_update = {'content-type': 'application/xml',
                                  'soapaction': 'Update' if len(update_employees) == 1 else 'UpdateList',
                                  'cache-control': 'no-cache'}
                r_update = response_to_json(requests.post(endpoint, headers=headers_update, data=payload_update))
                print(F"Updated:\n{r_update}")
            else:
                print('wrong')
        except Exception as e:
            print(r_dict)
            print(e)
            raise e


    dag << PythonOperator(
        task_id="refresh_employees",
        python_callable=refresh_employees
    )
