import requests
import logging
from time import sleep
from airflow.models import Variable


def trigger_dbt_job(message, steps=None, job_id='154605'):

    adhoc_job_endpoint = Variable.get("dbt_cloud_job_trigger_endpoint")

    adhoc_job_endpoint = adhoc_job_endpoint.replace('{job_id}', job_id)

    data = {"cause": message}
    if steps:
        data['steps_override'] = steps

    response = requests.post(adhoc_job_endpoint, headers={"Authorization": "Token " + Variable.get("DBT_API_KEY")},
                             json=data,
                             )
    try:
        response.raise_for_status()
        logging.info("Triggered dbt Cloud job")
    except Exception as e:
        raise Exception(f"Error triggering dbt Cloud job: {e}")

    run_id = response.json()['data']['id']
    get_run_endpoint = Variable.get("dbt_cloud_get_run_endpoint")

    get_run_endpoint = f"{get_run_endpoint}/{run_id}/"
    logging.info("Polling for job status")

    while True:

        run_response = requests.get(get_run_endpoint,
                                    headers={"Authorization": "Token " + Variable.get("DBT_API_KEY")}, )

        try:
            run_response.raise_for_status()
        except Exception as e:
            raise Exception(f"Error fetching run status: {e}")

        run_response = run_response.json()

        if run_response['data']['in_progress']:
            logging.info("Job still in progress. Sleeping for 30s before next poll")
            sleep(30)
        elif not run_response['data']['is_success']:
            raise Exception('dbt Cloud job failed')
        else:
            break

    logging.info("dbt Cloud job successfully finished")
