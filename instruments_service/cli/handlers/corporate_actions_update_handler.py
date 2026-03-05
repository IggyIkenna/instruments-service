"""
Corporate Actions Update Handler

Updates only outdated tickers (weekly incremental updates).
This is Phase 3 of the optimized corporate actions pipeline.

Key Features:
- Reads ticker_registry.yaml to find outdated tickers
- Fetches only tickers with last_download_date > threshold
- Appends new data to existing by_ticker CSVs
- Regenerates affected by_date files
- Updates metadata

Usage:
    python -m instruments_service \\
        --mode corporate_actions_update \\
        --days-threshold 7 \\
        --parallel-workers 10

Workflow:
    1. Load ticker_registry.yaml
    2. Filter tickers where last_download_date > 7 days
    3. Fetch ONLY those tickers (append mode)
    4. Update ticker_registry.yaml
    5. Regenerate affected date partitions
"""

import logging
from datetime import date
from pathlib import Path
from typing import cast

import yaml

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.cli.handlers.corporate_actions_backfill_handler import CorporateActionsBackfillHandler
from instruments_service.cli.handlers.generate_date_views_handler import GenerateDateViewsHandler

logger = logging.getLogger(__name__)


class CorporateActionsUpdateHandler(ModeHandler):
    """
    Handler for incremental updates of corporate actions.

    Only fetches tickers that are outdated (based on last_download_date).
    Much more efficient than re-fetching all tickers.

    Phase 3 of the optimized pipeline.
    """

    def __init__(self, config: dict[str, object]) -> None:
        super().__init__(config)

        # Reuse backfill handler for fetching
        self.backfill_handler = CorporateActionsBackfillHandler(config)

        # Reuse date views handler for regeneration
        self.date_views_handler = GenerateDateViewsHandler(config)

        logger.info("✅ CorporateActionsUpdateHandler initialized")

    def _load_ticker_registry(self, metadata_dir: Path) -> dict[str, object]:
        """Load ticker registry from YAML."""
        registry_path = metadata_dir / "ticker_registry.yaml"

        if not registry_path.exists():
            logger.warning("⚠️ Ticker registry not found: %s", registry_path)
            return {
                "version": "1.0",
                "tickers": {},
                "config": {
                    "update_frequency_days": 7,
                    "parallel_workers": 10,
                    "source": "yfinance",
                },
            }

        with open(registry_path) as f:
            registry: dict[str, object] = yaml.safe_load(f) or {}
        return registry

    def _get_outdated_tickers(
        self,
        registry: dict[str, object],
        days_threshold: int,
    ) -> list[str]:
        """
        Get list of tickers that need updating.

        Args:
            registry: Ticker registry dict
            days_threshold: Number of days before considering outdated

        Returns:
            List of ticker symbols that need updating
        """
        today = date.today()
        outdated: list[str] = []

        tickers_dict: dict[str, dict[str, object] | object] = cast(
            dict[str, dict[str, object] | object], registry.get("tickers") or {}
        )
        if not isinstance(tickers_dict, dict):
            return outdated

        for ticker_key, metadata_val in tickers_dict.items():
            ticker_str: str = str(ticker_key)
            metadata_dict: dict[str, object] = (
                cast(dict[str, object], metadata_val) if isinstance(metadata_val, dict) else {}
            )
            last_download_str: str | None = cast(str | None, metadata_dict.get("last_download_date"))

            # Never fetched
            if last_download_str is None:
                outdated.append(ticker_str)
                logger.debug("📋 %s: Never fetched", ticker_str)
                continue

            # Calculate days since last fetch
            try:
                last_download = (
                    date.fromisoformat(str(last_download_str)) if isinstance(last_download_str, str) else None
                )
                if last_download is None:
                    raise ValueError
                days_since = (today - last_download).days

                if days_since >= days_threshold:
                    outdated.append(ticker_str)
                    logger.debug("📋 %s: %s days old (threshold: %s)", ticker_str, days_since, days_threshold)

                # Also retry failed tickers (status == 'error')
                elif metadata_dict.get("status") == "error" and days_since >= 1:
                    outdated.append(ticker_str)
                    logger.debug("📋 %s: Retry failed ticker", ticker_str)

            except ValueError:
                logger.warning("⚠️ %s: Invalid date format: %s", ticker_str, last_download_str)
                outdated.append(ticker_str)

        return outdated

    def run(
        self,
        days_threshold: int = 7,
        parallel_workers: int = 10,
        upload_to_gcs: bool = False,
        max_retries: int = 3,
        regenerate_date_views: bool = True,
        **kwargs: object,
    ) -> dict[str, HandlerResultValue]:
        """
        Execute incremental corporate actions update.

        Args:
            days_threshold: Days before ticker is considered outdated (default: 7)
            parallel_workers: Number of parallel workers (default: 10)
            upload_to_gcs: Whether to upload to GCS (default: False for testing)
            max_retries: Maximum retry attempts per ticker (default: 3)
            regenerate_date_views: Whether to regenerate date views after update (default: True)

        Returns:
            Result dictionary with status and statistics
        """
        metadata_dir = Path("corporate_actions_backfill_output/metadata")

        # Load ticker registry
        logger.info("📂 Loading ticker registry...")
        registry = self._load_ticker_registry(metadata_dir)

        # Get outdated tickers
        logger.info("🔍 Finding tickers older than %s days...", days_threshold)
        outdated_tickers = self._get_outdated_tickers(registry, days_threshold)

        if not outdated_tickers:
            logger.info("✅ All tickers are up to date!")
            return {
                "success": True,
                "status": "success",
                "statistics": {
                    "tickers_checked": len(cast(dict[str, object], registry.get("tickers") or {})),
                    "tickers_outdated": 0,
                    "tickers_updated": 0,
                },
            }

        logger.info("📊 Found %s tickers to update", len(outdated_tickers))
        logger.info("⚙️ Examples: %s", f"{', '.join(outdated_tickers[:10])}")

        # Use backfill handler to fetch outdated tickers
        logger.info("\n🚀 Fetching outdated tickers...")
        result = self.backfill_handler.run(
            tickers=outdated_tickers,
            parallel_workers=parallel_workers,
            append_mode=True,  # Only append new data
            upload_to_gcs=upload_to_gcs,
            max_retries=max_retries,
        )

        stats_value = result.get("statistics")
        if stats_value is None:
            stats_value = {}
        stats = cast(dict[str, object], stats_value)

        # Regenerate date views if requested
        if regenerate_date_views:
            logger.info("\n🔄 Regenerating date views...")
            date_views_result = self.date_views_handler.run(
                upload_to_gcs=upload_to_gcs,
            )

            date_views_stats = date_views_result.get("statistics")
            if date_views_stats is None:
                date_views_stats = {}
            stats["date_views"] = date_views_stats

        logger.info("\n✅ Update complete")
        logger.info("📈 Updated %s/%s tickers", stats["tickers_successful"], len(outdated_tickers))

        _total_div = stats.get("total_dividends")
        _total_spl = stats.get("total_splits")
        _total_ear = stats.get("total_earnings")
        total_new_events = (
            (int(_total_div) if isinstance(_total_div, (int, float)) else 0)
            + (int(_total_spl) if isinstance(_total_spl, (int, float)) else 0)
            + (int(_total_ear) if isinstance(_total_ear, (int, float)) else 0)
        )
        return {
            "success": True,
            "status": "success",
            "statistics": {
                "tickers_checked": len(cast(dict[str, object], registry.get("tickers") or {})),
                "tickers_outdated": len(outdated_tickers),
                "tickers_updated": stats["tickers_successful"],
                "tickers_failed": stats["tickers_failed"],
                "total_new_events": total_new_events,
            },
        }

    def cleanup(self) -> None:
        """Cleanup resources."""
        self.backfill_handler.cleanup()
        self.date_views_handler.cleanup()
