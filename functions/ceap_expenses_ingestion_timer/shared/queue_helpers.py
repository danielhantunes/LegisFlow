from __future__ import annotations

import os

from azure.storage.queue import QueueClient, QueueServiceClient


def get_queue_client(queue_name: str) -> QueueClient:
    conn = os.environ["AzureWebJobsStorage"]
    qss = QueueServiceClient.from_connection_string(conn)
    client = qss.get_queue_client(queue_name)
    client.create_queue()
    return client


def send_json_message(queue_name: str, body: str, *, visibility_timeout: int | None = None) -> None:
    client = get_queue_client(queue_name)
    client.send_message(body, visibility_timeout=visibility_timeout)
