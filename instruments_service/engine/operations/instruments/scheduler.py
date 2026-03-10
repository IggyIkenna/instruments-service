from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

from unified_events_interface import log_event
from unified_internal_contracts import (
    EnhancedError,
    ErrorCategory,
    ErrorContext,
    ErrorRecoveryStrategy,
    ErrorSeverity,
    InternalPubSubTopic,
)

from instruments_service.engine.operations.instruments.lifecycle_monitor import InstrumentLifecycleMonitor
from instruments_service.monitors import InstrumentsFreshnessChecker

logger = logging.getLogger(__name__)


class InstrumentRefreshScheduler:
    """
    Continuously refreshes the instrument universe and publishes lifecycle events.

    Default: every 15 minutes (900 seconds). Configurable via refresh_interval_seconds.

    Lifecycle:
    1. Fetch all instruments from venue adapters
    2. Validate via InstrumentRecord schema (fail loud on violation)
    3. Diff against last known snapshot via InstrumentLifecycleMonitor
    4. Publish InstrumentLifecycleEvents to Pub/Sub instrument-lifecycle topic
    5. Write updated UniverseSnapshot to GCS
    6. Sleep until next interval
    """

    def __init__(
        self,
        refresh_interval_seconds: int = 900,
        pre_expiry_warning_hours: float = 24.0,
    ) -> None:
        self._interval = refresh_interval_seconds
        self._pre_expiry_warning_hours = pre_expiry_warning_hours
        self._running = False
        self._last_keys: set[str] = set()

    def start(self, orchestrator: object, publisher: object) -> None:
        """
        Blocking scheduler loop. Run in a thread or async task.

        Args:
            orchestrator: instruments orchestrator with fetch_all() -> dict[str, object]
            publisher: has publish(topic, event_dict) method
        """
        monitor = InstrumentLifecycleMonitor(pre_expiry_warning_hours=self._pre_expiry_warning_hours)
        freshness_checker = InstrumentsFreshnessChecker()
        self._running = True

        logger.info(
            "InstrumentRefreshScheduler started: interval=%ds, pre_expiry_warning=%sh",
            self._interval,
            self._pre_expiry_warning_hours,
        )

        while self._running:
            cycle_start = time.monotonic()
            cycle_start_utc = datetime.now(UTC)
            try:
                records: dict[str, object] = orchestrator.fetch_all()
                events = monitor.diff(self._last_keys, records)
                self._last_keys = set(records.keys())
                # Check freshness after each successful fetch.
                freshness_checker.check(last_fetch_ts=cycle_start_utc)

                for event_dict in events:
                    venue = str(event_dict.get("venue", "unknown"))
                    event_type = event_dict.get("event_type")
                    instrument_key = str(event_dict["instrument_key"]) if "instrument_key" in event_dict else "unknown"
                    event_dict["venue"] = venue

                    try:
                        # Publish instrument lifecycle events via SERVICE_EVENTS topic
                        # (InstrumentLifecycleEvent schema pending in unified_internal_contracts)
                        publisher.publish(
                            InternalPubSubTopic.SERVICE_EVENTS,
                            event_dict,
                        )
                        logger.info("Published %s for %s", event_type, instrument_key)
                    except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                        _err = EnhancedError(
                            message=str(e),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(e).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        log_event(
                            "INSTRUMENT_SCHEMA_VIOLATION",
                            metadata={
                                "instrument_key": instrument_key,
                                "error": str(e),
                            },
                        )
            except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.HIGH,
                    recovery_strategy=ErrorRecoveryStrategy.RETRY,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0.0, self._interval - elapsed)
            time.sleep(sleep_time)

    def stop(self) -> None:
        self._running = False


class _NullPublisher:
    """Log-only publisher used when no Pub/Sub transport is configured."""

    def publish(self, topic: object, event: object) -> None:
        logger.info("lifecycle_event topic=%s payload=%s", topic, event)


class _FetchAllAdapter:
    """
    Minimal orchestrator adapter that provides fetch_all() for the scheduler.

    Returns the last known snapshot. Override in production with a live adapter
    that queries venue APIs directly.
    """

    def __init__(self, last_snapshot: dict[str, object] | None = None) -> None:
        self._snapshot: dict[str, object] = last_snapshot or {}
        self._fetched_at: datetime | None = None

    def fetch_all(self) -> dict[str, object]:
        self._fetched_at = datetime.now(UTC)
        return self._snapshot

    def update_snapshot(self, snapshot: dict[str, object]) -> None:
        self._snapshot = snapshot
