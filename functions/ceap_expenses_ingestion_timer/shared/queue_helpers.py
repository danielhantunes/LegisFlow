from __future__ import annotations

import logging
import os
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient, QueueServiceClient

from .logger import get_logger, log_structured


def _queue_storage_connection_string() -> str:
    """Prefer dedicated setting used by queue triggers; fall back to host storage."""
    return os.getenv("CEAP_QUEUE_STORAGE") or os.environ["AzureWebJobsStorage"]


def create_queue_service_client() -> QueueServiceClient:
    """Single ``QueueServiceClient`` per process; supports longer connect timeouts for Functions."""
    conn = _queue_storage_connection_string()
    raw_timeout = os.getenv("CEAP_QUEUE_CONNECTION_TIMEOUT_SECONDS", "120")
    try:
        timeout = int(raw_timeout)
    except ValueError:
        timeout = 120
    try:
        return QueueServiceClient.from_connection_string(
            conn,
            connection_timeout=timeout,
        )
    except TypeError:
        # Older azure-storage-queue builds may not accept ``connection_timeout``.
        return QueueServiceClient.from_connection_string(conn)


# Process-wide reuse for ``send_json_message`` (e.g. replay worker) and optional sharing.
_queue_service_singleton: QueueServiceClient | None = None
_queues_create_called: set[str] = set()
_queue_standalone_logger = get_logger()


def _get_service_client_singleton() -> QueueServiceClient:
    global _queue_service_singleton
    if _queue_service_singleton is None:
        _queue_service_singleton = create_queue_service_client()
    return _queue_service_singleton


def ensure_queue_exists(
    client: QueueClient,
    *,
    logger: logging.Logger,
    queue_name: str,
    **ctx: Any,
) -> None:
    """Calls ``create_queue()`` once per logical setup; tolerates existing queue.

    Logs ``event=queue_exists_checked`` after the ensure attempt.
    """
    try:
        client.create_queue()
    except ResourceExistsError:
        pass
    log_structured(
        logger,
        "info",
        "Queue presence ensured (create idempotent).",
        event="queue_exists_checked",
        queue_name=queue_name,
        **ctx,
    )


def prepare_queue_client_for_dispatch(
    queue_name: str,
    *,
    logger: logging.Logger,
    **ctx: Any,
) -> QueueClient:
    """Builds a ``QueueClient`` and ensures the queue exists exactly once for this call.

    Intended for the CEAP dispatcher: call once before the enqueue loop, then reuse
    the returned client for every ``send_message``.

    Logs ``event=queue_client_created`` after setup.
    """
    service = create_queue_service_client()
    client = service.get_queue_client(queue_name)
    ensure_queue_exists(client, logger=logger, queue_name=queue_name, **ctx)
    log_structured(
        logger,
        "info",
        "Queue client ready for dispatch.",
        event="queue_client_created",
        queue_name=queue_name,
        **ctx,
    )
    return client


def send_json_message_with_client(
    queue_client: QueueClient,
    body: str,
    *,
    logger: logging.Logger,
    visibility_timeout: int | None = None,
    **ctx: Any,
) -> None:
    """Sends a message using a pre-built client (no ``create_queue`` here).

    Azure Functions queue triggers expect Base64-encoded payloads on the wire; the
    Azure Storage ``QueueClient.send_message`` default ``encode_message=True`` keeps
    that contract.

    On timeout-style failures, logs ``event=enqueue_timeout_error`` then re-raises.
    """
    log_structured(
        logger,
        "info",
        "Sending queue message.",
        event="message_enqueue_started",
        **ctx,
    )
    try:
        queue_client.send_message(body, visibility_timeout=visibility_timeout)
    except Exception as exc:
        ename = type(exc).__name__
        emessage = str(exc)
        is_timeout = (
            "Timeout" in ename
            or "timeout" in emessage.lower()
            or "timed out" in emessage.lower()
        )
        if is_timeout:
            log_structured(
                logger,
                "error",
                "Queue send timed out or connection timed out.",
                event="enqueue_timeout_error",
                error_type=ename,
                error_message=emessage,
                **ctx,
            )
        raise
    log_structured(
        logger,
        "info",
        "Queue message sent.",
        event="message_enqueue_finished",
        **ctx,
    )


def get_queue_client(queue_name: str) -> QueueClient:
    """Returns a ``QueueClient`` without calling ``create_queue`` (lazy ensure elsewhere)."""
    return _get_service_client_singleton().get_queue_client(queue_name)


def send_json_message(
    queue_name: str, body: str, *, visibility_timeout: int | None = None
) -> None:
    """Backward-compatible send: reuses one service client and ensures the queue once per process.

    Prefer ``prepare_queue_client_for_dispatch`` + ``send_json_message_with_client`` in hot loops.
    """
    client = get_queue_client(queue_name)
    if queue_name not in _queues_create_called:
        ensure_queue_exists(
            client, logger=_queue_standalone_logger, queue_name=queue_name
        )
        _queues_create_called.add(queue_name)
    send_json_message_with_client(
        client,
        body,
        logger=_queue_standalone_logger,
        visibility_timeout=visibility_timeout,
        queue_name=queue_name,
    )
