"""Live data source adapter — Pub/Sub subscriber for instruments-service."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import cast

from unified_cloud_interface import get_queue_client

logger = logging.getLogger(__name__)


class LiveDataSource:
    """Subscribes to a Pub/Sub topic and yields instrument update records for the engine."""

    def __init__(self, project_id: str, subscription_name: str) -> None:
        self._project_id = project_id
        self._subscription_name = subscription_name
        self._client = get_queue_client(project_id=project_id)

    async def stream(self) -> AsyncIterator[dict[str, object]]:
        """Yield deserialized records from the Pub/Sub subscription."""
        logger.info("Subscribing to %s", self._subscription_name)
        while True:
            loop = asyncio.get_event_loop()
            batch = await loop.run_in_executor(
                None,
                lambda: self._client.subscribe_once(self._subscription_name, timeout=5.0),
            )
            for data, _attrs in batch:
                yield self._deserialize(data)
            await asyncio.sleep(0.1)

    def _deserialize(self, data: bytes) -> dict[str, object]:
        parsed: object = cast(object, json.loads(data.decode("utf-8")))
        return cast(dict[str, object], parsed)

    def close(self) -> None:
        """No-op; client is managed by UCI factory cache."""
