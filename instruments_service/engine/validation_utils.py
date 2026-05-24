"""Validation utilities for instruments-service orchestrator.

Extracted from orchestrator.py to reduce file size and improve modularity.
These functions handle validation logic for dates, venues, and data quality.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def get_venue_epoch(venue: str) -> str | None:
    """Get the earliest date for which a venue has data."""
    # Known venue launch dates to avoid empty fetches
    venue_epochs = {
        "binance": "2017-07-14",
        "coinbase": "2012-10-01",
        "kraken": "2013-09-10",
        "bybit": "2018-03-01",
        "okx": "2014-01-01",
        "hyperliquid": "2023-05-01",
        "aster": "2024-01-01",
        "deribit": "2016-09-01",
        "uniswap_v3": "2021-05-05",
        "curve": "2020-01-01",
        "balancer": "2020-03-01",
        "sushi": "2020-08-01",
        "pancakeswap": "2020-09-01",
        # Sports venues
        "api_football": "2018-01-01",
        "soccerfootball_info": "2015-01-01",
        "footystats": "2015-01-01",
        # TradFi venues
        "ibkr": "2000-01-01",
        "databento": "2020-01-01",
    }

    return venue_epochs.get(venue.lower())


def should_skip_shard(
    date: str,
    venue: str,
    *,
    skip_weekends: bool = False,
    skip_before_epoch: bool = True,
) -> bool:
    """Determine if a shard should be skipped based on various criteria."""
    if skip_before_epoch:
        epoch = get_venue_epoch(venue)
        if epoch and date < epoch:
            logger.debug("Skipping %s for %s: before venue epoch %s", date, venue, epoch)
            return True

    if skip_weekends:
        try:
            dt = datetime.fromisoformat(date)
            if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
                logger.debug("Skipping %s for %s: weekend", date, venue)
                return True
        except ValueError:
            logger.warning("Invalid date format for weekend check: %s", date)

    return False


def should_skip_date_for_per_league(
    date: str,
    league_id: str,
    season_year: int | None = None,
) -> bool:
    """Determine if a date should be skipped for per-league processing."""
    try:
        dt = datetime.fromisoformat(date)
    except ValueError:
        logger.warning("Invalid date format: %s", date)
        return True

    # Skip dates too far in the future (more than 1 year ahead)
    if dt.year > datetime.now(UTC).year + 1:
        logger.debug("Skipping %s for league %s: too far in future", date, league_id)
        return True

    # Skip dates too far in the past for active leagues
    if dt.year < 2015:
        logger.debug("Skipping %s for league %s: too far in past", date, league_id)
        return True

    # Season-specific validation
    if season_year is not None and not (season_year - 1 <= dt.year <= season_year + 1):
        logger.debug("Skipping %s for league %s: outside season %d", date, league_id, season_year)
        return True

    return False


def classify_adapter_failure(exc: Exception, venue: str) -> str:
    """Classify adapter failure for error handling and retries."""
    exc_name = exc.__class__.__name__
    exc_msg = str(exc).lower()

    # Network/connection errors - usually transient
    if any(term in exc_name.lower() for term in ["connection", "timeout", "network"]):
        return "TRANSIENT_NETWORK"

    if any(term in exc_msg for term in ["timeout", "connection", "network"]):
        return "TRANSIENT_NETWORK"

    # Rate limiting
    if any(term in exc_msg for term in ["rate limit", "too many requests", "429"]):
        return "RATE_LIMITED"

    # Authentication issues
    if any(term in exc_msg for term in ["unauthorized", "forbidden", "401", "403", "invalid key"]):
        return "AUTH_FAILURE"

    # Data not found - may be expected for new venues/instruments
    if any(term in exc_msg for term in ["not found", "404", "no data"]):
        return "DATA_NOT_FOUND"

    # Server errors - may be transient
    if any(term in exc_msg for term in ["500", "502", "503", "504", "server error"]):
        return "SERVER_ERROR"

    # Client errors - likely permanent
    if any(term in exc_msg for term in ["400", "bad request", "invalid"]):
        return "CLIENT_ERROR"

    # Default to permanent for unknown errors
    return "UNKNOWN_ERROR"
