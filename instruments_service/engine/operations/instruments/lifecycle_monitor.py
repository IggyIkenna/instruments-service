from __future__ import annotations

import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


class InstrumentLifecycleMonitor:
    """
    Diffs two UniverseSnapshots to produce InstrumentLifecycleEvents.

    Called by InstrumentRefreshScheduler after each venue fetch.
    """

    def __init__(self, pre_expiry_warning_hours: float = 24.0) -> None:
        self._pre_expiry_warning_hours = pre_expiry_warning_hours

    def diff(self, old_keys: set[str], new_records: dict[str, object]) -> list[dict[str, object]]:
        """
        Returns list of lifecycle event dicts to be published.
        Fully typed version depends on InstrumentLifecycleEvent from UIC.
        Use dicts here to avoid import coupling — caller converts to schema.

        Args:
            old_keys: set of instrument_keys from previous snapshot
            new_records: dict of instrument_key -> InstrumentRecord from current fetch
        Returns:
            list of event dicts with keys: event_type, instrument_key, venue, effective_at,
            expiry, hours_until_expiry, new_status
        """
        from unified_internal_contracts.instrument_lifecycle import InstrumentLifecycleEventType
        from unified_internal_contracts.reference.instrument import InstrumentStatus

        now = datetime.now(UTC)
        events: list[dict[str, object]] = []
        new_keys = set(new_records.keys())

        for key in new_keys - old_keys:
            events.append(
                {
                    "event_type": InstrumentLifecycleEventType.LISTED,
                    "instrument_key": key,
                    "effective_at": now,
                    "new_status": InstrumentStatus.ACTIVE,
                }
            )

        for key in old_keys - new_keys:
            events.append(
                {
                    "event_type": InstrumentLifecycleEventType.DELISTED,
                    "instrument_key": key,
                    "effective_at": now,
                    "new_status": InstrumentStatus.DELISTED,
                }
            )

        for key, record in new_records.items():
            expiry = getattr(record, "expiry", None)
            if expiry is None:
                continue
            if hasattr(expiry, "tzinfo") and expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)

            hours_remaining = (expiry - now).total_seconds() / 3600

            if hours_remaining <= 0:
                events.append(
                    {
                        "event_type": InstrumentLifecycleEventType.EXPIRED,
                        "instrument_key": key,
                        "effective_at": now,
                        "expiry": expiry,
                        "hours_until_expiry": 0.0,
                        "new_status": InstrumentStatus.EXPIRED,
                    }
                )
            elif hours_remaining <= self._pre_expiry_warning_hours:
                events.append(
                    {
                        "event_type": InstrumentLifecycleEventType.EXPIRING_SOON,
                        "instrument_key": key,
                        "effective_at": now,
                        "expiry": expiry,
                        "hours_until_expiry": round(hours_remaining, 2),
                        "new_status": InstrumentStatus.ACTIVE,
                    }
                )

        return events
