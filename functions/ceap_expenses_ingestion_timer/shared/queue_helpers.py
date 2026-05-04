from __future__ import annotations

import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient, QueueServiceClient


def _queue_storage_connection_string() -> str:
    """Prefer dedicated setting used by queue triggers; fall back to host storage."""
    return os.getenv("CEAP_QUEUE_STORAGE") or os.environ["AzureWebJobsStorage"]


def get_queue_client(queue_name: str) -> QueueClient:
    conn = _queue_storage_connection_string()
    qss = QueueServiceClient.from_connection_string(conn)
    client = qss.get_queue_client(queue_name)
    try:
        client.create_queue()
    except ResourceExistsError:
        # Idempotent creation: queue may already exist from Terraform/provisioning.
        pass
    return client


def send_json_message(queue_name: str, body: str, *, visibility_timeout: int | None = None) -> None:
    client = get_queue_client(queue_name)
    client.send_message(body, visibility_timeout=visibility_timeout)
