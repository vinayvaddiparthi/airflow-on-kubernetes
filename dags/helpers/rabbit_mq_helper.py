import logging
import json
import pika

from airflow.models.dagrun import DagRun
from typing import Any


def notify_subscribers(
    rabbit_url: str, exchange_label: str, topic: str, dag_run: DagRun, **kwargs: Any,
) -> None:
    if "merchant_guid" in dag_run.conf:
        merchant_guid = dag_run.conf["merchant_guid"]

        logging.info(
            f"Notifying subscribers cash flow projection is complete for {merchant_guid}"
        )

        params = pika.URLParameters(rabbit_url)

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        message = {"merchant_guid": merchant_guid}

        channel.exchange_declare(
            exchange=exchange_label, exchange_type="topic", durable=True
        )
        channel.basic_publish(
            exchange=exchange_label,
            routing_key=topic,
            body=json.dumps(message),
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

        logging.info(f"✔️ MQ sent {topic}: {merchant_guid}")
    else:
        logging.warning("No subscribers notified, DAG was run without context.")
