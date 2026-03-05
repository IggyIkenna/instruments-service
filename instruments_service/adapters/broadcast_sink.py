"""Broadcast sink adapter — Pub/Sub publisher for instruments-service."""

from __future__ import annotations

import json
import logging

from unified_cloud_interface import get_queue_client

logger = logging.getLogger(__name__)


class BroadcastSink:
    """Publishes processed instrument records to a Pub/Sub topic."""

    def __init__(self, project_id: str, topic_name: str) -> None:
        self._project_id = project_id
        self._topic_name = topic_name
        self._client = get_queue_client(project_id=project_id)

    def publish(self, record: dict[str, object]) -> None:
        """Serialize and publish a single instrument record."""
        data = json.dumps(record).encode("utf-8")
        self._client.publish(self._topic_name, data)
        logger.debug("Published to %s", self._topic_name)

    def close(self) -> None:
        """No-op; client is managed by UCI factory cache."""
