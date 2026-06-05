"""InstrumentsHandler — mode-agnostic handler for the instruments operation.

Preflight starts an ApiKeyReloader that fetches keys from Secret Manager
and periodically refreshes them (hot-reload on key rotation). Missing keys
fail the shard at startup — not after silently producing zero results.

In CLOUD_MOCK_MODE, key validation is skipped (no real Secret Manager available).
"""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import (
    ApiKeyReloader,
    BatchPayload,
    ManifestWriter,
    ServiceRuntime,
    UnifiedServiceHandler,
    classify_and_emit_error,
    get_storage_client,
    publish_coordination_event,  # pyright: ignore[reportPrivateImportUsage]
    resolve_bucket_name,
)

from instruments_service.engine import orchestrator as engine_orchestrator
from instruments_service.engine.orchestrator import (
    clear_defi_universe_cache,
    earliest_venue_date,
    get_venues_for_asset_groups,
    resolve_instruments_store_kind,
)

logger = logging.getLogger(__name__)


def _get_instruments_bucket_for_asset_group(asset_group: str) -> str:
    """Get the instruments bucket for the given asset group using public API.

    Prediction resolves via the dedicated flat kind ``instruments-store-prediction``
    (the bucket-name SSOT omits a ``PREDICTION`` entry from the per-asset_group
    ``instruments-store`` dict — see :func:`resolve_instruments_store_kind`).
    """
    normalized_group = asset_group.lower()
    kind, kind_ag = resolve_instruments_store_kind(normalized_group)
    return resolve_bucket_name(cloud="gcp", kind=kind, asset_group=kind_ag)


def _get_instruments_bucket(asset_group: str = "SPORTS") -> str:
    """Legacy alias for _get_instruments_bucket_for_asset_group."""
    return _get_instruments_bucket_for_asset_group(asset_group)


class InstrumentsHandler(UnifiedServiceHandler):
    """Process canonical instrument records via URDI for a date and asset-group set.

    preflight():
      1. Starts ApiKeyReloader (fail-fast on missing keys, periodic refresh)
      2. Checks which dates already have complete data (skips those in process())

    process(payload): calls engine orchestrator for the given date+asset groups
    """

    # Args property is set by the service framework adapter
    args: argparse.Namespace | None

    def __init__(self, runtime: ServiceRuntime) -> None:
        super().__init__(runtime)
        self._completed_dates: set[str] = set()
        self._key_reloader: ApiKeyReloader | None = None
        self._venue_override: list[str] | None = None  # set in preflight() when --venues is used
        self._sports_entity_filter: str | None = None  # set in preflight() when --sports-entity is used
        self._sports_provider: str | None = None  # set in preflight() when --sports-provider is used
        self._league_filter: list[str] | None = None  # set in preflight() when --league is used
        self._season_override: int | None = None  # set in preflight() when --season is used
        # Recovery-mode fixture allowlist (set in preflight() when
        # --recovery-fixture-ids is used). When non-None, the orchestrator's
        # per-fixture entity handlers filter fixture_ids to this set BEFORE
        # calling api_football, and the per-league parquet writes use
        # read-modify-write semantics to preserve existing fixtures' rows.
        self._recovery_fixture_ids: frozenset[int] | None = None
        # Live-mode trigger name (Phase A.7 of instruments_master).
        # When set under --mode live, names which entity-type subset to refresh +
        # which source adapter to invoke. Closed-set per-asset-group taxonomy lives
        # in UAC (Phase A.6); until that lands the field accepts any string and the
        # downstream dispatcher (Phase B.1+) validates the name fail-loud. Stored
        # at handler-construction time so process() can route on it once the Phase
        # B/C/D/E triggers wire in. None ⇒ batch-mode invocation OR live-mode call
        # without an explicit trigger (legacy sports forward-poll launchers).
        self._trigger_name: str | None = None

    async def preflight(self) -> None:
        """Start API key reloader. Date/asset-group filtering happens in process()."""
        _args = self.args
        cli_asset_groups: list[str] | None = None
        if _args is not None:
            cli_asset_groups = getattr(_args, "asset_group", None) or getattr(_args, "category", None)
        asset_groups = cli_asset_groups or ["ALL"]

        is_all = any(c.upper() == "ALL" for c in asset_groups)
        if is_all or any(c.upper() == "DEFI" for c in asset_groups):
            clear_defi_universe_cache()

        self._wire_cli_filters_from_args()

        active_venues = get_venues_for_asset_groups(asset_groups)
        self._start_key_reloader(active_venues)

    def _wire_cli_filters_from_args(self) -> None:
        """Map instruments CLI flags from parsed args onto handler fields."""
        venues_arg: list[str] | None = getattr(self.args, "venues", None) if self.args else None
        if venues_arg:
            self._venue_override = venues_arg
            logger.info("Venue override from CLI: %s", venues_arg)
            earliest = earliest_venue_date(venues_arg)
            if earliest:
                logger.info("Earliest venue launch date: %s (dates before this will be skipped)", earliest)

        sports_entity_arg: str | None = getattr(self.args, "sports_entity", None) if self.args else None
        if sports_entity_arg:
            self._sports_entity_filter = sports_entity_arg
            logger.info(
                "Sports entity filter from CLI: %s (only this entity will be checked/fetched)", sports_entity_arg
            )

        sports_provider_arg: str | None = getattr(self.args, "sports_provider", None) if self.args else None
        if sports_provider_arg:
            self._sports_provider = sports_provider_arg.upper()
            logger.info("Sports provider filter from CLI: %s (only this provider will run)", self._sports_provider)

        league_arg: str | None = getattr(self.args, "league", None) if self.args else None
        if league_arg:
            self._league_filter = [lid.strip() for lid in league_arg.split(",") if lid.strip()]
            logger.info("League filter from CLI: %s", self._league_filter)

        season_arg: int | None = getattr(self.args, "season", None) if self.args else None
        if season_arg is not None:
            self._season_override = season_arg
            logger.info("Season override from CLI: %d", season_arg)

        recovery_path_arg: str | None = getattr(self.args, "recovery_fixture_ids", None) if self.args else None
        if recovery_path_arg:
            self._recovery_fixture_ids = self._load_recovery_fixture_ids(recovery_path_arg)

        trigger_arg: str | None = getattr(self.args, "trigger", None) if self.args else None
        if trigger_arg:
            self._trigger_name = trigger_arg
            logger.info(
                "Live-mode trigger from CLI: %s (downstream dispatcher routes by name)",
                trigger_arg,
            )

        lookback_arg: int | None = getattr(self.args, "lookback_days", None) if self.args else None
        lookahead_arg: int | None = getattr(self.args, "lookahead_days", None) if self.args else None
        force_window_arg: bool = bool(getattr(self.args, "force_window", False)) if self.args else False
        if lookback_arg is not None or lookahead_arg is not None or force_window_arg:
            logger.info(
                "Rolling-window CLI: lookback_days=%s lookahead_days=%s force_window=%s "
                "(resolved to --start-date=%s --end-date=%s)",
                lookback_arg,
                lookahead_arg,
                force_window_arg,
                getattr(self.args, "start_date", None),
                getattr(self.args, "end_date", None),
            )

    def _load_recovery_fixture_ids(self, path: str) -> frozenset[int] | None:
        """Load af_fixture_id allowlist from a parquet at GCS or local path.

        The parquet must carry a column named ``af_fixture_id`` (the
        recovery-set parquet emitted by Phase 1's audit script and consumed
        by Phase 2's recovery script both have this column). Returns
        ``None`` on any load failure — the caller treats None as "no
        allowlist, run normally."
        """
        try:
            if path.startswith("gs://"):  # noqa: gs-uri
                without_scheme = path[len("gs://") :]  # noqa: gs-uri
                bucket, _, blob = without_scheme.partition("/")
                if not bucket or not blob:
                    logger.error(
                        "--recovery-fixture-ids: malformed gs:// path %r — expected gs://bucket/path/to.parquet",
                        path,
                    )
                    return None
                storage = get_storage_client()
                raw_bytes = storage.download_bytes(bucket=bucket, blob_path=blob)
                df = pd.read_parquet(io.BytesIO(raw_bytes))
            else:
                df = pd.read_parquet(path)
        except Exception as exc:
            logger.error("--recovery-fixture-ids: failed to read parquet at %r: %s", path, exc)
            return None
        if "af_fixture_id" not in df.columns:
            logger.error(
                "--recovery-fixture-ids: parquet at %r is missing required ``af_fixture_id`` column (found: %s)",
                path,
                list(df.columns),
            )
            return None
        ids = pd.to_numeric(df["af_fixture_id"], errors="coerce").dropna().astype(int)
        allowlist = frozenset(int(x) for x in ids.tolist() if int(x) > 0)
        logger.info(
            "Recovery fixture-id allowlist loaded from %s: %d unique af_fixture_ids — "
            "per-fixture entity loops will filter to this set, date-level pre-flight bypassed",
            path,
            len(allowlist),
        )
        return allowlist if allowlist else None

    def _start_key_reloader(self, active_venues: list[str]) -> None:
        """Start API key reloader — fail-fast on missing keys, periodic refresh."""
        try:
            self._key_reloader = ApiKeyReloader(
                venues=active_venues,
                project_id=self.runtime.gcp_project_id or None,
            )
            self._key_reloader.start()
            keys = self._key_reloader.current_keys
            if keys:
                logger.info(
                    "API keys validated for %d data source(s): %s",
                    len(keys),
                    sorted(keys.keys()),
                )
        except Exception as _exc:
            classify_and_emit_error(
                _exc,
                service_name="instruments-service",
                operation="api_key_validation",
                reraise=True,
            )

    async def process(self, payload: BatchPayload) -> object:
        """Process instruments for the date in the payload.

        Reads API keys from the hot-reloader (always fresh after rotation).
        Skip-if-exists is handled by the orchestrator's check_shard_freshness()
        which uses the manifest with per-asset-group buckets (correct bucket resolution).
        """
        date = str(payload.date) if not isinstance(payload.date, str) else payload.date
        # Normalize datetime to YYYY-MM-DD string (BatchIO yields datetime objects)
        if "T" in date or " " in date:
            date = date[:10]
        redo_all = payload.force or bool(payload.extra.get("redo_all", False))

        asset_groups: list[str] = list(payload.asset_groups) if payload.asset_groups else ["ALL"]
        api_keys = self._key_reloader.current_keys if self._key_reloader else {}
        return await engine_orchestrator.process_instruments(
            date=date,
            asset_groups=asset_groups,
            redo_all=redo_all,
            api_keys=api_keys,
            venue_override=self._venue_override,
            mode=str(self.runtime.mode),
            sports_entity_filter=self._sports_entity_filter,
            sports_provider=self._sports_provider,
            league_filter=self._league_filter,
            season_override=self._season_override,
            recovery_fixture_ids=self._recovery_fixture_ids,
        )

    async def cleanup(self) -> None:
        """Flush any buffered manifest writes to GCS at end of batch."""
        flushed: list[str] = []
        for asset_group in ("SPORTS", "CEFI", "DEFI", "TRADFI"):
            try:
                bucket = _get_instruments_bucket_for_asset_group(asset_group)
                if bucket:
                    writer = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
                    writer.flush()
                    flushed.append(bucket)
            except Exception as exc:
                logger.warning("ManifestWriter final flush failed for %s: %s", asset_group, exc)
        if flushed:
            logger.info("ManifestWriter cleanup: flushed buffers for %s", flushed)

        # Publish DATA_READY coordination event so downstream services
        # (e.g. market-tick-data-service) know instrument data is available.
        # Only fires in live mode; publish_coordination_event raises ValueError
        # in batch mode so we guard with suppress.
        with contextlib.suppress(RuntimeError, ValueError):
            publish_coordination_event(
                "DATA_READY",
                payload={
                    "timestamp": datetime.now(UTC).isoformat(),
                    "data_type": "instruments",
                    "service": "instruments-service",
                },
            )

        # Publish sports live stats availability for WebSocket forwarding
        _args2 = self.args
        cli_asset_groups_cleanup: list[str] | None = None
        if _args2 is not None:
            cli_asset_groups_cleanup = getattr(_args2, "asset_group", None) or getattr(_args2, "category", None)
        if cli_asset_groups_cleanup and any(c.upper() in ("SPORTS", "ALL") for c in cli_asset_groups_cleanup):
            with contextlib.suppress(RuntimeError, ValueError):
                publish_coordination_event(
                    "SPORTS_LIVE_STATS",
                    payload={
                        "timestamp": datetime.now(UTC).isoformat(),
                        "data_type": "live_stats",
                        "service": "instruments-service",
                    },
                )
