"""Instruments metadata freshness checking for instruments-service.

Call ``InstrumentsFreshnessChecker.check(last_fetch_ts)`` after each
instrument universe refresh. The checker uses a FreshnessMonitor (from
unified-trading-library) with the ``instruments-service`` contract and
delegates to ``log_if_stale()`` which emits DATA_STALE or FEED_UNHEALTHY
automatically.

Instruments metadata does not have a per-venue entry in MARKET_TICK_FRESHNESS;
it lives in FEATURE_FRESHNESS under the service name. A sensible default
contract (hourly cadence, 2-hour max age) is used when no entry is found so
the service can start without contract-registry changes.

Typical usage in InstrumentRefreshScheduler:

    checker = InstrumentsFreshnessChecker()
    # ... refresh cycle ...
    checker.check(last_fetch_ts=cycle_start_utc)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from unified_internal_contracts import FEATURE_FRESHNESS, DataFreshnessContract
from unified_trading_library import FreshnessMonitor

logger = logging.getLogger(__name__)

# Fallback contract for instruments metadata (not in MARKET_TICK_FRESHNESS).
# Instruments universe refreshes every 15 minutes; warn at 30 min, critical at 2 h.
_DEFAULT_INSTRUMENTS_CONTRACT = DataFreshnessContract(
    source="instruments-service",
    asset_class="feature",
    max_age_seconds=7200,  # 2 hours
    warn_age_seconds=1800,  # 30 minutes
    expected_cadence_seconds=900,  # 15 minutes
    criticality="important",
)

_SERVICE_KEY = "instruments-service"


class InstrumentsFreshnessChecker:
    """Freshness checker for the instruments universe metadata feed.

    Emits DATA_STALE or FEED_UNHEALTHY events via log_event() when the
    instruments universe has not been refreshed within its SLA.
    """

    def __init__(self) -> None:
        contract: DataFreshnessContract = FEATURE_FRESHNESS.get(_SERVICE_KEY, _DEFAULT_INSTRUMENTS_CONTRACT)
        self._monitor = FreshnessMonitor(contract=contract)
        logger.debug(
            "InstrumentsFreshnessChecker initialised: warn=%ds max=%ds criticality=%s",
            contract.warn_age_seconds,
            contract.max_age_seconds,
            contract.criticality,
        )

    @property
    def contract(self) -> DataFreshnessContract:
        """Expose the underlying freshness contract (read-only)."""
        return self._monitor.contract

    def check(self, last_fetch_ts: datetime) -> None:
        """Check freshness of the instruments universe and emit events if stale.

        Emits:
        - ``DATA_STALE`` (WARNING) when warn threshold is breached
        - ``FEED_UNHEALTHY`` (ERROR) when max threshold is breached

        No event is emitted when the feed is within SLA.

        Args:
            last_fetch_ts: UTC timestamp of the most recent successful
                           instrument universe fetch. Naive datetimes are
                           interpreted as UTC.
        """
        now = datetime.now(UTC)
        ts = last_fetch_ts.replace(tzinfo=UTC) if last_fetch_ts.tzinfo is None else last_fetch_ts
        age_seconds = (now - ts).total_seconds()
        self._monitor.log_if_stale(source=_SERVICE_KEY, last_update_seconds_ago=age_seconds)
