import requests
import logging
from time import sleep
from airflow.models import Variable


def trigger_dbt_job(message, steps=None, job_id="154605", polling_frequency=30):

    adhoc_job_endpoint = Variable.get("dbt_cloud_job_trigger_endpoint")

    adhoc_job_endpoint = adhoc_job_endpoint.replace("{job_id}", job_id)

    dbt_api_key = Variable.get("dbt_api_key")

    data = {"cause": message}
    if steps:
        data["steps_override"] = steps

    response = requests.post(
        adhoc_job_endpoint,
        headers={"Authorization": "Token " + dbt_api_key},
        json=data,
    )
    try:
        response.raise_for_status()
        response = response.json()
        trigged_job_url = response["data"]["href"]
        logging.info(f"Triggered dbt Cloud job: {trigged_job_url}")
    except Exception as e:
        raise Exception(f"Error triggering dbt Cloud job: {e}")

    run_id = response["data"]["id"]
    get_run_endpoint = Variable.get("dbt_cloud_get_run_endpoint")

    get_run_endpoint = f"{get_run_endpoint}/{run_id}/"
    logging.info("Polling for job status")

    while True:

        run_response = requests.get(
            get_run_endpoint,
            headers={"Authorization": "Token " + dbt_api_key},
        )

        try:
            run_response.raise_for_status()
        except Exception as e:
            raise Exception(f"Error fetching run status: {e}")

        run_response = run_response.json()

        if run_response["data"]["in_progress"]:
            logging.info(
                f"Job still in progress. Sleeping for {polling_frequency}s before next poll"
            )
            sleep(polling_frequency)
        elif not run_response["data"]["is_success"]:
            raise Exception("dbt Cloud job failed")
        else:
            break

    logging.info("dbt Cloud job successfully finished")
