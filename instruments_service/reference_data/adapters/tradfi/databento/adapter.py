"""Databento SDK adapter class — curated futures/options/equities fetch.

Cohesion module of the ``adapters.tradfi.databento`` package (split from the
former monolithic ``adapters/tradfi/databento.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators (the ``db`` SDK module alias, ``log_event``,
``classify_venue_error``, the symbology / session helpers) resolve through
``_db`` — the live package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.tradfi.databento.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split.
"""

# Package-internal access: the databento package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from unified_api_contracts import (
    FX_SPOT_PAIRS,
    KNOWN_ETFS,
    TRADFI_DATABENTO_INSTRUMENTS,
    TRADFI_TICKER_UNIVERSE,
    CanonicalFuturesContract,
    FuturesContractLifecyclePhase,
)
from unified_api_contracts.internal import AssetClass, InstrumentLeg, InstrumentRecord, InstrumentType, OptionType

from ....base_adapter import BaseReferenceDataAdapter
from ....schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

if TYPE_CHECKING:
    from instruments_service.reference_data.adapters.tradfi import databento as _db
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.tradfi.databento._pkg_ref import databento_namespace as _db

__all__ = [
    "DatabentoReferenceDataAdapter",
]


class DatabentoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Databento reference data adapter using the official SDK.

    Fetches curated instruments from TRADFI_DATABENTO_INSTRUMENTS (UAC registry)
    using symbol-level queries (stype_in=parent for futures/options), plus
    S&P 500 / ETF equities from TRADFI_TICKER_UNIVERSE.

    FX spot pairs are static InstrumentRecords (not from Databento).
    """

    def __init__(
        self,
        project_id: str | None = None,
        datasets: list[str] | None = None,
        target_date: date | None = None,
        api_key: str | None = None,
        venue_filter: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._target_date: date = target_date or date.today()
        self._venue_filter: str | None = venue_filter

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._venue_filter or "databento"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        api_key = self._optional_api_key()
        if api_key is None:
            raise ValueError(
                "api_key required — service must fetch databento-api-key from "
                "Secret Manager and pass it via api_key= constructor parameter."
            )

        results: list[InstrumentRecord] = []
        vf = self._venue_filter

        # 1. Fetch curated futures/options from TRADFI_DATABENTO_INSTRUMENTS
        #    Filter to only instruments matching the requested venue.
        #    Grouped by (dataset, stype_in) to batch API calls.
        filtered_defs = [d for d in TRADFI_DATABENTO_INSTRUMENTS if vf is None or d.venue == vf]
        if not filtered_defs and vf:
            _db.logger.info("Databento: no instruments registered for venue %s", vf)

        groups: dict[tuple[str, str], list[str]] = {}
        for inst_def in filtered_defs:
            key = (inst_def.dataset, inst_def.stype_in)
            groups.setdefault(key, []).append(inst_def.symbol)

        # DATASET-level shard isolation (operator 2026-06-18 subscription cutover):
        # a single venue (e.g. NASDAQ) groups instruments across SEVERAL datasets —
        # an off-allowlist one (XNAS.ITCH for IBIT/ETHA, IFEU/IFUS for ICE) raises a
        # DatabentoSubscriptionError from _fetch_symbols. That is a PERMANENT condition
        # (we will never be entitled to it) and MUST isolate to its own dataset so the
        # sibling allowed datasets (DBEQ.BASIC / GLBX.MDP3 / CFE) still return — the
        # per-dataset fetch IS the shard. The breach already emitted ADAPTER_FETCH_FAILED
        # inside _fetch_symbols; here we log + continue. A TRANSIENT failure (BentoError /
        # parse — re-raised as a plain RuntimeError by _fetch_symbols) is NOT caught here:
        # it propagates to fail the whole venue → _fetch_one_venue's failed[] → manifest
        # attempted_failed + retry (CF-11). SSOT: codex/04-architecture/shard-level-failure-isolation.md.
        from unified_api_contracts.registry import DatabentoSubscriptionError  # noqa: qg-inside-import

        for (dataset, stype_in), symbols in groups.items():
            _db.logger.info(
                "Databento [%s]: fetching %d symbols from %s (stype=%s)...",
                vf or "ALL",
                len(symbols),
                dataset,
                stype_in,
            )
            try:
                batch = self._fetch_symbols(api_key, dataset, symbols, stype_in)
            except DatabentoSubscriptionError as _ds_exc:
                _db.logger.error(
                    "Databento [%s]: dataset %s off-allowlist (isolated — siblings continue): %s",
                    vf or "ALL",
                    dataset,
                    _ds_exc,
                )
                continue
            _db.logger.info("Databento [%s]: %s returned %d instruments", vf or "ALL", dataset, len(batch))
            results.extend(batch)

        # 2. Fetch S&P 500 equities + ETFs — only for NASDAQ/NYSE venues
        if vf in (None, "NASDAQ", "NYSE"):
            equity_symbols = self._get_equity_symbols()
            if equity_symbols:
                _db.logger.info(
                    "Databento [%s]: fetching %d equity/ETF symbols from DBEQ.BASIC...",
                    vf or "ALL",
                    len(equity_symbols),
                )
                try:
                    batch = self._fetch_symbols(api_key, "DBEQ.BASIC", equity_symbols, "raw_symbol")
                except DatabentoSubscriptionError as _eq_exc:
                    _db.logger.error(
                        "Databento [%s]: DBEQ.BASIC equity fetch off-allowlist (isolated): %s",
                        vf or "ALL",
                        _eq_exc,
                    )
                    batch = []
                _db.logger.info("Databento [%s]: DBEQ.BASIC returned %d instruments", vf or "ALL", len(batch))
                results.extend(batch)

        # 3. Static FX spot pairs — only for FX venue
        if vf in (None, "FX"):
            results.extend(self._create_fx_spot_records())

        # 3b. Static Yahoo Finance indices — venue-driven (CBOE=VIX, ICE=DXY, …)
        from unified_api_contracts.registry import YAHOO_INDICES as _YAHOO_INDICES

        _yahoo_venues = {idx.venue for idx in _YAHOO_INDICES}
        if vf is None or vf in _yahoo_venues:
            results.extend(self._create_yahoo_index_records(venue_filter=vf))

        # 3c. Static KRX (Korea Exchange) single stocks — Yahoo-sourced (.KS), only
        #     for the KRX venue (2026-06-24 close-out).
        if vf in (None, "KRX"):
            results.extend(self._create_krx_equity_records())

        # 4. Enrich with session metadata (trading hours, holidays, early closes)
        self._enrich_session_metadata(results)

        if instrument_type is not None:
            results = [r for r in results if r.instrument_type == instrument_type]

        _db.logger.info(
            "Databento adapter: %d total instruments (%d futures/options, equities, FX)",
            len(results),
            len(results),
        )
        return results

    def _enrich_session_metadata(self, results: list[InstrumentRecord]) -> None:
        """Enrich records with session metadata (trading hours, holidays, early closes).

        Computed once per venue, then applied to all records for that venue.
        """
        session_cache: dict[str, dict[str, str | bool | None]] = {}
        for record in results:
            venue = record.venue
            if venue not in session_cache:
                session_cache[venue] = _db._get_session_metadata(venue, self._target_date)
            meta = session_cache[venue]
            if meta:
                record.is_trading_day = meta.get("is_trading_day")
                record.regular_open_utc = meta.get("regular_open_utc")
                record.regular_close_utc = meta.get("regular_close_utc")
                record.early_close_utc = meta.get("early_close_utc")
                record.pre_market_open_utc = meta.get("pre_market_open_utc")
                record.post_market_close_utc = meta.get("post_market_close_utc")
                record.auction_open_utc = meta.get("auction_open_utc")
                record.auction_close_utc = meta.get("auction_close_utc")
                record.holiday_calendar = meta.get("holiday_calendar")

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        instruments = await self.get_instruments(instrument_type="OPTION")
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            und = inst.underlying or inst.base_asset or ""
            if underlying.upper() not in und.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.option_type == OptionType.CALL:
                calls.append(inst)
            elif inst.option_type == OptionType.PUT:
                puts.append(inst)
            if inst.strike is not None:
                strikes.add(inst.strike)
        now = datetime.now(UTC)
        target_expiry = expiry or (calls[0].expiry if calls else now)
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=target_expiry or now,
            strikes=sorted(strikes),
            calls=calls,
            puts=puts,
            fetched_at=now,
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        instruments = await self.get_instruments(instrument_type=instrument_type)
        expiry_set: set[datetime] = set()
        for inst in instruments:
            und = inst.underlying or inst.base_asset or ""
            if underlying.upper() not in und.upper():
                continue
            if inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_canonical_futures_contracts(
        self,
        venue: str | None = None,
        underlying: str | None = None,
    ) -> list[CanonicalFuturesContract]:
        """Return CanonicalFuturesContract records for all known futures roots/months.

        Conservative lifecycle date mapping: all 5 required date fields are set to
        inst.expiry.date() since Databento DEFINITION records carry only a single
        expiry timestamp.  Per Phase 4.1 plan (tradfi_canonical_futures_contract_
        hard_required_fields_2026_05_13): this conservative approach is correct for
        initial rollout; per-venue refinement (distinct LTD/FND/delivery) is a
        separate follow-up.

        Args:
            venue: Optional venue filter (e.g. "CME", "ICE"). Defaults to all venues.
            underlying: Optional root/underlying filter (e.g. "ES", "CL").
        """
        instruments = await self.get_instruments(instrument_type="FUTURE")
        today = date.today()
        result: list[CanonicalFuturesContract] = []
        for inst in instruments:
            if inst.expiry is None:
                continue
            root = _db._extract_underlying_from_symbol(inst.raw_symbol) or (inst.underlying or "")
            if not root:
                continue
            inst_venue = inst.venue or self.venue
            if venue is not None and inst_venue.upper() != venue.upper():
                continue
            if underlying is not None and root.upper() != underlying.upper():
                continue
            expiry_dt = inst.expiry
            expiry_d = expiry_dt.date() if isinstance(expiry_dt, datetime) else expiry_dt
            phase = FuturesContractLifecyclePhase.EXPIRED if today > expiry_d else FuturesContractLifecyclePhase.ACTIVE
            listed_at = inst.available_from_datetime
            if listed_at is not None and listed_at.tzinfo is None:
                listed_at = listed_at.replace(tzinfo=UTC)
            with contextlib.suppress(Exception):
                result.append(
                    CanonicalFuturesContract(
                        venue=inst_venue,
                        root=root,
                        contract_symbol=inst.raw_symbol,
                        contract_month=expiry_d.month,
                        contract_year=expiry_d.year,
                        expiry_date=expiry_d,
                        last_trading_date=expiry_d,
                        first_notice_date=expiry_d,
                        delivery_date=expiry_d,
                        settlement_date=expiry_d,
                        lifecycle_phase=phase,
                        tick_size=inst.tick_size,
                        contract_size=inst.contract_size,
                        listed_at=listed_at,
                    )
                )
        return result

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Databento does not provide funding rates (equity/futures only)")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError(
            "Databento OHLCV requires timeseries.get_range with DBN binary format. "
            "Use the databento Python SDK directly for this operation."
        )

    # ------------------------------------------------------------------
    # Private: fetch helpers
    # ------------------------------------------------------------------

    def _get_equity_symbols(self) -> list[str]:
        """Build the equity symbol list from TRADFI_TICKER_UNIVERSE.

        Includes sp500 + nasdaq + nyse-tradfi-perp single stocks + ETFs. The
        nasdaq + nyse-tradfi-perp lists were previously omitted, so NASDAQ-only
        names (HOOD/INTC/RIVN/UBER/CRWD/MRVL/ZM, etc.) declared in the universe
        were never actually fetched from DBEQ.BASIC — leaving the captured
        tradfi equity universe a strict subset of the enumerated one and
        failing the Binance TradFi-perp basis-arb superset invariant
        (operator 2026-06-24: "some extra ones are fine, but NOT LESS").
        """
        sp500 = TRADFI_TICKER_UNIVERSE.get("sp500_tickers", [])
        nasdaq = TRADFI_TICKER_UNIVERSE.get("nasdaq_tickers", [])
        nyse_perp = TRADFI_TICKER_UNIVERSE.get("nyse_tradfi_perp_tickers", [])
        etfs = TRADFI_TICKER_UNIVERSE.get("etf_tickers", [])
        # Deduplicate, preserving order
        seen: set[str] = set()
        symbols: list[str] = []
        for s in [*sp500, *nasdaq, *nyse_perp, *etfs]:
            if s not in seen:
                seen.add(s)
                symbols.append(s)
        return symbols

    def _fetch_symbols(
        self,
        api_key: str,
        dataset: str,
        symbols: list[str],
        stype_in: str,
    ) -> list[InstrumentRecord]:
        """Fetch specific symbols from a Databento dataset.

        Uses timeseries.get_range(schema=DEFINITION, symbols=..., stype_in=...)
        to fetch only the requested instruments instead of the entire dataset.
        """
        client = _db.db.Historical(api_key)
        target = self._target_date
        # Databento has T+2 embargo — cap the query date to 3 days before today
        today = date.today()
        effective_date = min(target, today - timedelta(days=3))
        # Equity datasets (DBEQ) have no data on weekends/holidays.
        # Use a 5-day window ending on effective_date to catch the nearest trading day.
        # Futures (GLBX, IFEU) trade Sunday evening so a 1-day window suffices.
        is_equity_dataset = dataset.startswith("DBEQ")
        lookback = timedelta(days=5) if is_equity_dataset else timedelta(days=0)
        start = datetime(effective_date.year, effective_date.month, effective_date.day, tzinfo=UTC) - lookback
        end = datetime(effective_date.year, effective_date.month, effective_date.day, tzinfo=UTC) + timedelta(days=1)

        # Subscription-entitlement gate (operator 2026-06-18): the request's
        # (dataset, schema='definition' → L0, start) tuple MUST fall inside the paid
        # 3-dataset subscription + the schema's free included-history window, else the
        # query would be billed pay-as-you-go. Fail CLOSED — a disallowed request never
        # reaches the vendor. This is a 403/ENTITLEMENT breach (NOT 402/PAYG); classify
        # + emit ADAPTER_FETCH_FAILED + re-raise the DatabentoSubscriptionError as-is so
        # get_instruments's per-dataset loop can ISOLATE it (a PERMANENT off-allowlist
        # condition — never retried; sibling datasets still return) — distinct from the
        # transient BentoError/parse RuntimeError below which fails the whole venue → the
        # _fetch_one_venue failed[] retry path (attempted_failed). DatabentoSubscriptionError
        # IS a RuntimeError subclass, so the loop catches it FIRST (before plain RuntimeError).
        # SSOT: UAC registry/databento_subscription_allowlist.py + shard-level-failure-isolation.md.
        from unified_api_contracts.registry import (  # noqa: qg-inside-import
            DatabentoSubscriptionError,
            assert_databento_request_allowed,
        )

        try:
            assert_databento_request_allowed(dataset, "definition", start.isoformat())
        except DatabentoSubscriptionError as _ent_exc:
            classification = _db.classify_venue_error("DATABENTO", "DATABENTO_ENTITLEMENT")
            _db.logger.error(
                "Databento entitlement breach dataset=%s schema=definition start=%s: %s",
                dataset,
                start.date(),
                _ent_exc,
            )
            _db.log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "DATABENTO",
                    "dataset": dataset,
                    "schema": "definition",
                    "symbol_count": len(symbols),
                    "error": str(_ent_exc),
                    "error_code": "DATABENTO_ENTITLEMENT",
                    "action": classification.action.value if classification else "fail",
                    "retry_safe": False,
                },
            )
            raise

        try:
            data = client.timeseries.get_range(
                dataset=dataset,
                schema="definition",
                symbols=symbols,
                stype_in=stype_in,
                start=start.isoformat(),
                end=end.isoformat(),
            )
        except _db.db.common.error.BentoError as exc:
            error_code = _db._classify_bento_error(exc)
            classification = _db.classify_venue_error("DATABENTO", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            _db.logger.error(
                "Databento SDK error dataset %s symbols=%d: %s (classified: %s, action: %s)",
                dataset,
                len(symbols),
                exc,
                error_code,
                action,
            )
            _db.log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "DATABENTO",
                    "dataset": dataset,
                    "symbol_count": len(symbols),
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # Re-raise as RuntimeError so urdi_reference_provider._fetch_one's
            # RuntimeError handler catches it and records the venue in failed[],
            # ensuring the manifest layer sees attempted_failed (not clean empty).
            # Shard isolation is preserved: the raise is caught by _fetch_one's
            # per-venue except ladder — sibling venues are unaffected.
            raise RuntimeError(
                f"Databento fetch failed for dataset={dataset} "
                f"(error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc

        try:
            df = data.to_df()
        except Exception as _exc:
            _db.logger.warning("Failed to parse Databento DBN data for %s: %s", dataset, _exc)
            _db.log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "DATABENTO",
                    "dataset": dataset,
                    "symbol_count": len(symbols),
                    "error": str(_exc),
                    "error_code": "PARSE_ERROR",
                    "action": "fail",
                    "retry_safe": False,
                },
            )
            # Re-raise so urdi_reference_provider._fetch_one records this venue
            # in failed[] (→ attempted_failed in manifest).  A parse failure is
            # NOT a genuine empty — we cannot determine the actual symbol list.
            raise RuntimeError(f"Databento DBN parse failure for dataset={dataset}: {_exc}") from _exc

        if df.empty:
            _db.logger.info(
                "No instrument definitions found in %s for %d symbols on %s",
                dataset,
                len(symbols),
                target,
            )
            return []

        _db.logger.info(
            "Fetched %d instrument definitions from %s (%d symbols requested)",
            len(df),
            dataset,
            len(symbols),
        )
        canonical_venue = _db._DATASET_TO_VENUE.get(dataset, dataset)

        # Pre-collect leg data for spread instruments (ICE populates leg fields,
        # CME does not — leg_count=0 for all CME instruments).
        combo_legs: dict[str, list[InstrumentLeg]] = {}
        if "leg_count" in df.columns:
            spread_rows = df[df["leg_count"] > 0]
            for sym, grp in spread_rows.groupby("raw_symbol"):
                legs: list[InstrumentLeg] = []
                for _, leg_row in grp.sort_values("leg_index").iterrows():
                    leg_sym = str(getattr(leg_row, "leg_raw_symbol", "") or "").strip()
                    if not leg_sym:
                        continue
                    side_raw = str(getattr(leg_row, "leg_side", "B") or "B").strip()
                    side = "SELL" if side_raw in ("A", "S") else "BUY"
                    ratio_num = int(getattr(leg_row, "leg_ratio_qty_numerator", 1) or 1)
                    ratio_den = int(getattr(leg_row, "leg_ratio_qty_denominator", 1) or 1)
                    ratio = max(ratio_num // ratio_den, 1) if ratio_den else ratio_num
                    # Resolve leg instrument_key — the leg is a separate instrument
                    # in the same venue. Determine its type from instrument_class.
                    leg_class = str(getattr(leg_row, "leg_instrument_class", "F") or "F")
                    leg_type = _db._CLASS_TO_TYPE.get(leg_class, InstrumentType.FUTURE)
                    leg_key = f"{canonical_venue}:{leg_type}:{leg_sym}"
                    legs.append(InstrumentLeg(instrument_key=leg_key, side=side, ratio=ratio))
                if legs:
                    combo_legs[str(sym)] = legs

        results: list[InstrumentRecord] = []
        seen_symbols: set[str] = set()

        for _, row in df.iterrows():
            raw_sym = str(getattr(row, "raw_symbol", "") or "")
            # For multi-row spreads (ICE), only process the first row per symbol.
            # Leg data was already collected above.
            if raw_sym in seen_symbols and raw_sym in combo_legs:
                continue
            record = self._parse_row_to_record(
                row,
                dataset,
                canonical_venue,
                combo_legs.get(raw_sym),
            )
            if record is None:
                continue
            # Deduplicate equities: DBEQ.BASIC returns multiple rows per symbol
            # (one per exchange listing). Keep first occurrence only.
            if stype_in == "raw_symbol" and record.raw_symbol in seen_symbols:
                continue
            seen_symbols.add(record.raw_symbol)
            results.append(record)
        return results

    def _create_fx_spot_records(self) -> list[InstrumentRecord]:
        """Create static InstrumentRecords for FX spot pairs (Yahoo Finance data)."""
        records: list[InstrumentRecord] = []
        for fx in FX_SPOT_PAIRS:
            symbol = f"{fx.base}-{fx.quote}"
            records.append(
                InstrumentRecord(
                    instrument_key=f"FX:SPOT_PAIR:{symbol}",
                    venue="FX",
                    asset_group=AssetClass.FX,
                    instrument_type=InstrumentType.SPOT_PAIR,
                    raw_symbol=fx.yahoo_ticker,
                    base_asset=fx.base,
                    quote_asset=fx.quote,
                    tick_size=Decimal("0.0001"),
                    min_size=Decimal("1"),
                    contract_size=Decimal("1"),
                    available_from_datetime=datetime(2020, 1, 1, tzinfo=UTC),
                    timezone="UTC",
                    holiday_calendar="FX",
                )
            )
        return records

    def _create_krx_equity_records(self) -> list[InstrumentRecord]:
        """Create static InstrumentRecords for KRX single stocks (Yahoo Finance .KS).

        venue=KRX, instrument_type=EQUITY, asset_group=EQUITY. The 3 Korean
        underliers of the Binance tradfi-perps (HYUNDAI 005380 / SAMSUNG 005930 /
        SKHYNIX 000660) — added 2026-06-24 (KRX venue close-out). Genesis is the
        per-entry Yahoo history floor (never a shared hardcoded date). The bare KRX
        numeric code is the canonical symbol; the ``.KS`` Yahoo ticker is raw_symbol.
        """
        from unified_api_contracts.registry import KRX_EQUITIES

        records: list[InstrumentRecord] = []
        for eq in KRX_EQUITIES:
            genesis = eq.first_available_date
            records.append(
                InstrumentRecord(
                    instrument_key=f"KRX:EQUITY:{eq.symbol}",
                    venue="KRX",
                    asset_group=AssetClass.EQUITY,
                    instrument_type=InstrumentType.EQUITY,
                    raw_symbol=eq.yahoo_ticker,
                    base_asset=eq.symbol,
                    quote_asset="KRW",
                    tick_size=Decimal("1"),
                    min_size=Decimal("1"),
                    contract_size=Decimal("1"),
                    available_from_datetime=datetime(genesis.year, genesis.month, genesis.day, tzinfo=UTC),
                    timezone="Asia/Seoul",
                    holiday_calendar="KRX",
                )
            )
        return records

    def _create_yahoo_index_records(self, venue_filter: str | None = None) -> list[InstrumentRecord]:
        """Create static InstrumentRecords for Yahoo Finance indices (VIX, DXY, etc.).

        venue_filter=None returns all indices; otherwise only those for the given venue.
        """
        from unified_api_contracts.registry import YAHOO_INDICES

        records: list[InstrumentRecord] = []
        for idx in YAHOO_INDICES:
            if venue_filter is not None and idx.venue != venue_filter:
                continue
            # Resolve timezone from exchange hours config (same as Databento-sourced instruments)
            venue_hours = _db._EXCHANGE_HOURS.get(idx.venue)
            tz = venue_hours["tz"] if venue_hours and venue_hours.get("tz") else "UTC"
            # Canonical key carries the base-quote suffix (CBOE:INDEX:VIX-USD) — it
            # MUST match the GCS/symbology key and the data_source_continuity
            # resolver key, else get_source_for_instrument() silently returns None.
            quote = "USD"
            # Genesis = the instrument's empirically-confirmed first Yahoo bar,
            # carried per-entry on YahooIndexDef (never a shared hardcoded date).
            genesis = idx.first_available_date
            records.append(
                InstrumentRecord(
                    instrument_key=f"{idx.venue}:INDEX:{idx.base_asset}-{quote}",
                    venue=idx.venue,
                    asset_group=AssetClass(idx.asset_group),
                    instrument_type=InstrumentType.INDEX,
                    raw_symbol=idx.yahoo_ticker,
                    base_asset=idx.base_asset,
                    quote_asset=quote,
                    timezone=tz,
                    available_from_datetime=datetime(genesis.year, genesis.month, genesis.day, tzinfo=UTC),
                    # INDEX instruments are non-tradeable pricing references —
                    # tick_size/min_size/contract_size not meaningful but set
                    # for schema completeness.
                    tick_size=Decimal("0.01"),
                    min_size=Decimal("1"),
                    contract_size=Decimal("1"),
                )
            )
        return records

    # ------------------------------------------------------------------
    # Private: parsing
    # ------------------------------------------------------------------

    def _parse_tick_and_lot(self, row: object) -> tuple[Decimal, Decimal]:
        """Extract tick size and lot size from a DataFrame row."""
        tick_raw = getattr(row, "min_price_increment", None)
        try:
            tick_val = Decimal(str(tick_raw)) if tick_raw else Decimal("0.01")
            tick_size = tick_val if tick_val.is_finite() and tick_val > 0 else Decimal("0.01")
        except Exception as _exc:
            tick_size = Decimal("0.01")
        lot_raw = getattr(row, "min_lot_size_round_lot", None)
        try:
            lot_val = Decimal(str(lot_raw)) if lot_raw else Decimal("1")
            lot_size = lot_val if lot_val.is_finite() and lot_val > 0 else Decimal("1")
        except Exception as _exc:
            lot_size = Decimal("1")
        return tick_size, lot_size

    @staticmethod
    def _parse_expiry_from_row(row: object) -> datetime | None:
        """Parse expiry datetime from a DataFrame row."""
        expiry_raw = getattr(row, "expiration", None)
        if expiry_raw is None:
            return None
        with contextlib.suppress(ValueError, TypeError):
            return datetime.fromisoformat(str(expiry_raw).replace("Z", "+00:00")).astimezone(UTC)
        return None

    @staticmethod
    def _parse_strike_from_row(row: object) -> Decimal | None:
        """Parse strike price from a DataFrame row."""
        strike_raw = getattr(row, "strike_price", None)
        if strike_raw is None:
            return None
        try:
            val = Decimal(str(strike_raw))
            return val if val.is_finite() else None
        except Exception as _exc:
            return None

    @staticmethod
    def _parse_option_type_from_row(row: object, inst_class: str) -> str | None:
        """Parse option type from a DataFrame row, falling back to instrument_class."""
        option_type_raw = str(getattr(row, "option_type", "") or "").upper()
        if not option_type_raw and inst_class in ("C", "P"):
            option_type_raw = inst_class
        return option_type_raw or None

    def _is_filtered_out(self, dataset: str, inst_class: str, expiry: datetime | None) -> bool:
        """Check if a row should be filtered out (expired, too far out)."""
        if expiry is not None and expiry.date() < self._target_date:
            return True
        max_expiry = self._target_date + timedelta(days=365)
        return expiry is not None and expiry.date() > max_expiry

    def _parse_row_to_record(
        self,
        row: object,
        dataset: str,
        canonical_venue: str,
        pre_parsed_legs: list[InstrumentLeg] | None = None,
    ) -> InstrumentRecord | None:
        """Parse a single DataFrame row into an InstrumentRecord."""
        raw_symbol = str(getattr(row, "raw_symbol", "") or getattr(row, "symbol", "") or "")
        if not raw_symbol:
            return None

        inst_class = str(getattr(row, "instrument_class", "E"))
        instrument_type = _db._CLASS_TO_TYPE.get(inst_class, InstrumentType.SPOT_PAIR)
        # Databento returns CME event contracts (EC* roots) as instrument_class="BAG"
        if inst_class == "BAG" and raw_symbol[:2] == "EC":
            instrument_type = InstrumentType.EVENT_CONTRACT
        currency = str(getattr(row, "currency", "USD") or "USD")

        expiry = self._parse_expiry_from_row(row)
        strike = self._parse_strike_from_row(row)
        option_type = self._parse_option_type_from_row(row, inst_class)
        underlying = str(getattr(row, "underlying", "") or "")
        # Databento doesn't always populate `underlying` for futures/options.
        # Derive from raw_symbol using registered exchange codes (parent symbols).
        if not underlying and instrument_type in (InstrumentType.FUTURE, InstrumentType.OPTION) and raw_symbol:
            underlying = _db._extract_underlying_from_symbol(raw_symbol)
        if not underlying and instrument_type == InstrumentType.EVENT_CONTRACT and raw_symbol:
            underlying = raw_symbol.split("-")[0]  # "ECBTC-EOM-2026-05-30-0.5" → "ECBTC"
        tick_size, lot_size = self._parse_tick_and_lot(row)

        if self._is_filtered_out(dataset, inst_class, expiry):
            return None

        # CME class "S" from futures datasets = exchange-defined calendar spreads.
        # Parse legs from raw_symbol (e.g. "ESM6-ESU6" → BUY ESM6 + SELL ESU6).
        # Class S from equity datasets (DBEQ) remains SPOT_PAIR.
        if inst_class == "S" and dataset in _db._FUTURES_DATASETS:
            instrument_type = InstrumentType.COMBO
            if pre_parsed_legs is None:
                pre_parsed_legs = _db._parse_cme_calendar_spread_legs(raw_symbol, canonical_venue)

        # User-defined combos/spreads (e.g. "UD:1V:CXT ...") come through as
        # futures/options from parent symbology but have no derivable underlying.
        # Reclassify them as COMBO instruments.
        if instrument_type in (InstrumentType.FUTURE, InstrumentType.OPTION) and not underlying:
            instrument_type = InstrumentType.COMBO

        # Determine asset class from the UAC registry per-instrument, not per-dataset.
        # Build lookup: exchange_code → asset_group from the curated registry.
        asset_group = self._resolve_asset_group(dataset, raw_symbol, underlying)
        if dataset == "DBEQ.BASIC" and raw_symbol in KNOWN_ETFS:
            instrument_type = InstrumentType.ETF

        # For equity-venue instruments, route to correct canonical venue
        if dataset == "DBEQ.BASIC":
            nasdaq_tickers = set(TRADFI_TICKER_UNIVERSE.get("nasdaq_tickers", []))
            canonical_venue = "NASDAQ" if raw_symbol in nasdaq_tickers else "NYSE"

        # Parse available_since from Databento activation timestamp.
        # Not all datasets populate activation (DBEQ.BASIC, IFEU/IFUS).
        # For futures/options: estimate from expiry (listing period heuristic).
        # For equities/spot: fall back to venue-level floor date.
        activation_raw = getattr(row, "activation", None)
        available_since: datetime | None = None
        if activation_raw is not None:
            with contextlib.suppress(ValueError, TypeError):
                available_since = datetime.fromisoformat(str(activation_raw).replace("Z", "+00:00")).astimezone(UTC)
        if available_since is None:
            available_since = self._estimate_available_since(
                instrument_type,
                expiry,
                canonical_venue,
            )

        # Resolve timezone from exchange hours config
        venue_hours = _db._EXCHANGE_HOURS.get(canonical_venue)
        tz = venue_hours["tz"] if venue_hours and venue_hours.get("tz") else "UTC"

        is_combo = instrument_type == InstrumentType.COMBO

        # COMBO instruments: only emit when real legs are available.
        # ICE provides leg data (leg_count > 0) → pre_parsed_legs is populated.
        # CME has leg_count=0 → no leg data → skip the combo entirely.
        if is_combo and not pre_parsed_legs:
            return None
        legs = pre_parsed_legs if is_combo else None

        # Canonical product identity (additive — raw_symbol stays the raw code).
        # Resolve the human product root from the existing UAC exchange-code
        # registry (no Databento API call). COMBO/spread instruments span >1
        # product, so they carry no single canonical root.
        product_root = None if is_combo else _db._resolve_product_root(raw_symbol)
        canonical_instrument_id = self._build_canonical_instrument_id(
            canonical_venue=canonical_venue,
            instrument_type=instrument_type,
            product_root=product_root,
            expiry=expiry,
            strike=strike if not is_combo else None,
            option_type=option_type if not is_combo else None,
        )

        return InstrumentRecord(
            instrument_key=f"{canonical_venue}:{instrument_type.upper()}:{raw_symbol}",
            venue=canonical_venue,
            asset_group=asset_group,
            raw_symbol=raw_symbol,
            instrument_type=instrument_type,
            base_asset=underlying or raw_symbol,
            quote_asset=currency,
            product_root=product_root,
            canonical_instrument_id=canonical_instrument_id,
            tick_size=tick_size if not is_combo else None,
            min_size=lot_size if not is_combo else None,
            contract_size=Decimal("1") if not is_combo else None,
            expiry=expiry,
            strike=strike if not is_combo else None,
            option_type=(
                {"C": OptionType.CALL, "P": OptionType.PUT}.get(option_type) if option_type and not is_combo else None
            ),
            underlying=underlying or None,
            legs=legs,
            available_from_datetime=available_since,
            timezone=tz,
        )

    @staticmethod
    def _estimate_available_since(
        instrument_type: str,
        expiry: datetime | None,
        canonical_venue: str,
    ) -> datetime:
        """Estimate available_since when Databento doesn't populate activation.

        For futures/options with an expiry: approximate listing date as
        expiry minus a venue-specific listing period. CME lists standard
        futures ~18 months out, ICE ~12 months. Options typically list
        closer to expiry.

        For equities/spot without expiry: fall back to venue floor date.
        """
        if expiry is not None and instrument_type in (
            InstrumentType.FUTURE,
            InstrumentType.OPTION,
            InstrumentType.EVENT_CONTRACT,
        ):
            # Listing period heuristic by venue
            if instrument_type == InstrumentType.EVENT_CONTRACT:
                listing_months = 1  # CME EC* daily binaries list ~30 days before resolution
            elif instrument_type == InstrumentType.OPTION:
                listing_months = 6  # options list closer to expiry
            elif canonical_venue == "CME":
                listing_months = 18  # CME standard futures
            elif canonical_venue == "ICE":
                listing_months = 12  # ICE futures
            else:
                listing_months = 12  # conservative default
            estimated = expiry - timedelta(days=listing_months * 30)
            # Don't go before the venue floor
            floor = _db._VENUE_FLOOR_DATES.get(canonical_venue, _db._DEFAULT_TRADFI_FLOOR)
            return max(estimated, floor)
        return _db._VENUE_FLOOR_DATES.get(canonical_venue, _db._DEFAULT_TRADFI_FLOOR)

    @staticmethod
    def _resolve_asset_group(dataset: str, raw_symbol: str, underlying: str) -> AssetClass:
        """Resolve asset_group per-instrument from the UAC registry.

        Checks the underlying (parent symbol from Databento, e.g. "ES", "CL", "6E")
        against the curated registry. Falls back to exchange code prefix extraction,
        then to dataset-level mapping.
        """
        # 1. Try underlying directly (best match for parent-stype queries)
        if underlying:
            ac = _db._EXCHANGE_CODE_asset_group.get(underlying)
            if ac:
                return AssetClass(ac)

        # 2. Try known exchange code prefixes (longest match first)
        # Handles "ESM6" → "ES", "6EZ6" → "6E", "BTCM6" → "BTC"
        for length in (3, 2):
            if len(raw_symbol) >= length:
                prefix = raw_symbol[:length]
                ac = _db._EXCHANGE_CODE_asset_group.get(prefix)
                if ac:
                    return AssetClass(ac)

        # 3. Fallback to dataset-level mapping
        return _db._DATASET_TO_asset_group.get(dataset, AssetClass.EQUITY)

    @staticmethod
    def _build_canonical_instrument_id(
        canonical_venue: str,
        instrument_type: InstrumentType,
        product_root: str | None,
        expiry: datetime | None,
        strike: Decimal | None,
        option_type: str | None,
    ) -> str | None:
        """Build a human-canonical instrument id from the resolved product root.

        Shape: ``{venue}:{instrument_type}:{product_root}:{expiry_or_tenor}[:{strike}{C|P}]``
        e.g. ``CME:FUTURE:SP500:2030-06`` / ``CME:OPTION:SP500:2025-10:5000C``.

        Returns ``None`` when no product root resolves — the id is additive and
        must not fabricate identity for instruments the registry can't canonicalise.
        """
        if not product_root:
            return None
        parts = [canonical_venue, instrument_type.upper(), product_root]
        if expiry is not None:
            parts.append(expiry.strftime("%Y-%m"))
        if strike is not None and instrument_type == InstrumentType.OPTION:
            strike_str = format(strike.normalize(), "f")
            suffix = {"C": "C", "P": "P"}.get((option_type or "").upper(), "")
            parts.append(f"{strike_str}{suffix}")
        return ":".join(parts)
