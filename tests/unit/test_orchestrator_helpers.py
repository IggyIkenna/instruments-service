"""Tests for orchestrator helper functions - no cloud/network dependencies."""

from __future__ import annotations

import io
import tempfile
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import (
    _build_defi_venues,
    _extract_fixture_venue_ids,
    _get_instruments_bucket,
    _load_venue_coordinates,
    _validate_predictions_null_rates,
    _write_catalogue_record,
    _write_fixture_mapping,
    _write_venue,
    filter_defi_instruments_by_relevance,
    filter_instruments_by_date,
    get_venues_for_asset_groups,
    is_venue_available,
    reject_junk_instruments,
)


def _orchestrator_package_source() -> str:
    """Concatenated source of the orchestrator package + all cohesion submodules.

    The former monolithic ``engine/orchestrator.py`` was split into the
    ``engine/orchestrator/`` package (codex_violations_ratchet_to_five_2026_06_10);
    source-scan regression tests must see the WHOLE package, not just
    ``__init__.py``, to keep their protective power.
    """
    import inspect
    import pkgutil

    from instruments_service.engine import orchestrator

    sources = [inspect.getsource(orchestrator)]
    for mod_info in pkgutil.iter_modules(orchestrator.__path__):
        sources.append(inspect.getsource(getattr(orchestrator, mod_info.name)))
    return "\n".join(sources)


def _make_record(
    instrument_key: str = "TEST",
    venue: str = "TEST-VENUE",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "ETH",
    quote_asset: str = "USDT",
    available_since: datetime | None = None,
    available_to: datetime | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue=venue,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        available_from_datetime=available_since,
        available_to_datetime=available_to,
    )


# ---------------------------------------------------------------------------
# reject_junk_instruments — §1.5/G1.4 capture-time noise guard
# ---------------------------------------------------------------------------


class TestRejectJunkInstruments:
    """Non-ASCII / known-test instruments are dropped at capture time (G1.4)."""

    def test_cjk_base_asset_is_rejected(self) -> None:
        """The 2026-06-24 audit junk (龙虾/币安人生/我踏马来了) is rejected by non-ASCII base."""
        records = [
            _make_record(instrument_key="BITGET-FUTURES:PERPETUAL:龙虾-USDT", venue="BITGET-FUTURES", base_asset="龙虾"),
            _make_record(instrument_key="ASTER:PERP:我踏马来了USDT", venue="ASTER", base_asset="我踏马来了"),
            _make_record(instrument_key="BINANCE-SPOT:SPOT_PAIR:币安人生-USDT", venue="BINANCE-SPOT", base_asset="币安人生"),
        ]
        kept = reject_junk_instruments(records)
        assert kept == []

    def test_non_ascii_in_raw_symbol_or_key_is_rejected(self) -> None:
        """Junk caught even when the non-ASCII char is in raw_symbol / instrument_key."""
        r = _make_record(instrument_key="ASTER:PERP:龙虾USDT", venue="ASTER", base_asset="LOBSTER")
        r.raw_symbol = "龙虾USDT"
        assert reject_junk_instruments([r]) == []

    def test_known_test_base_is_rejected(self) -> None:
        """An ASCII known-test base (TEST/DUMMY) is rejected."""
        records = [
            _make_record(instrument_key="V:SPOT:TEST-USDT", venue="V", base_asset="TEST"),
            _make_record(instrument_key="V:SPOT:DUMMY-USDT", venue="V", base_asset="DUMMY"),
        ]
        assert reject_junk_instruments(records) == []

    def test_legitimate_instruments_pass_through(self) -> None:
        """Normal ASCII instruments (incl. Binance stocks AAPL/XAU) are kept."""
        records = [
            _make_record(instrument_key="BINANCE-FUTURES:PERPETUAL:BTC-USDT", venue="BINANCE-FUTURES", base_asset="BTC"),
            _make_record(instrument_key="BINANCE-FUTURES:PERPETUAL:AAPL-USDT", venue="BINANCE-FUTURES", base_asset="AAPL"),
            _make_record(instrument_key="BINANCE-FUTURES:PERPETUAL:XAU-USDT", venue="BINANCE-FUTURES", base_asset="XAU"),
        ]
        kept = reject_junk_instruments(records)
        assert len(kept) == 3

    def test_mixed_keeps_only_clean(self) -> None:
        """A mixed batch keeps the clean records and drops only the junk."""
        good = _make_record(instrument_key="BINANCE-SPOT:SPOT_PAIR:ETH-USDT", venue="BINANCE-SPOT", base_asset="ETH")
        junk = _make_record(instrument_key="BINANCE-SPOT:SPOT_PAIR:币安人生-USDT", venue="BINANCE-SPOT", base_asset="币安人生")
        kept = reject_junk_instruments([good, junk])
        assert kept == [good]


# ---------------------------------------------------------------------------
# _canonical_manifest_venue_chain — on-chain CeFi perps must NOT defi-split (G1.3)
# ---------------------------------------------------------------------------


class TestCanonicalManifestVenueChainCefiOnChain:
    """On-chain CeFi perp CLOBs keep their full venue + chain="" (asset_group=cefi)."""

    def test_on_chain_cefi_perps_not_split_to_defi_shape(self) -> None:
        """LIGHTER-ZKSYNC / PACIFICA-SOLANA / EXTENDED-STARKNET → (full_venue, "").

        Regression for the G1.3 320-row contamination: the manifest writer split
        these glued cefi venues on their KNOWN_CHAIN suffix → manifest
        asset_group=defi + chain=<L2>. They are cefi venues (VENUE_TO_ASSET_GROUP=="cefi",
        like HYPERLIQUID/ASTER) and MUST carry chain="" so _cat resolves to cefi.
        """
        from instruments_service.engine.orchestrator import _canonical_manifest_venue_chain

        for venue in ("LIGHTER-ZKSYNC", "PACIFICA-SOLANA", "EXTENDED-STARKNET"):
            mv, mc = _canonical_manifest_venue_chain(venue)
            assert (mv, mc) == (venue, ""), f"{venue}: expected ({venue!r}, '') got ({mv!r}, {mc!r})"

    def test_real_defi_pool_venue_still_splits(self) -> None:
        """A genuine DeFi PROTOCOL-CHAIN venue still splits to bare protocol + chain."""
        from instruments_service.engine.orchestrator import _canonical_manifest_venue_chain

        assert _canonical_manifest_venue_chain("AAVE_V3-ETHEREUM") == ("AAVE_V3", "ETHEREUM")
        assert _canonical_manifest_venue_chain("UNISWAP_V3-ARBITRUM") == ("UNISWAP_V3", "ARBITRUM")

    def test_plain_cefi_venue_unchanged(self) -> None:
        """A plain cefi/tradfi venue (no DeFi split) passes through with chain=""."""
        from instruments_service.engine.orchestrator import _canonical_manifest_venue_chain

        assert _canonical_manifest_venue_chain("BINANCE-FUTURES") == ("BINANCE-FUTURES", "")
        assert _canonical_manifest_venue_chain("HYPERLIQUID") == ("HYPERLIQUID", "")


# ---------------------------------------------------------------------------
# get_venues_for_asset_groups
# ---------------------------------------------------------------------------


class TestGetVenuesForCategories:
    def test_cefi_returns_cefi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["CEFI"])
        assert "BINANCE-SPOT" in venues
        assert "DERIBIT" in venues
        assert "HYPERLIQUID" in venues

    def test_defi_returns_defi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["DEFI"])
        assert any("AAVE_V3" in v for v in venues)
        assert any("UNISWAP_V3" in v for v in venues)

    def test_tradfi_returns_tradfi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["TRADFI"])
        assert "CME" in venues
        assert "NASDAQ" in venues

    def test_sports_returns_api_football(self) -> None:
        venues = get_venues_for_asset_groups(["SPORTS"])
        assert "API_FOOTBALL" in venues

    def test_prediction_returns_polymarket_kalshi(self) -> None:
        venues = get_venues_for_asset_groups(["PREDICTION"])
        assert "POLYMARKET" in venues
        assert "KALSHI" in venues

    def test_all_includes_all_categories(self) -> None:
        venues = get_venues_for_asset_groups(["ALL"])
        assert "BINANCE-SPOT" in venues
        assert "CME" in venues
        assert "API_FOOTBALL" in venues
        assert "POLYMARKET" in venues
        assert any("AAVE_V3" in v for v in venues)

    def test_empty_categories_returns_empty(self) -> None:
        venues = get_venues_for_asset_groups([])
        assert venues == []

    def test_deduplication(self) -> None:
        venues = get_venues_for_asset_groups(["CEFI", "CEFI"])
        # Should not have duplicates
        assert len(venues) == len(set(venues))

    def test_case_insensitive(self) -> None:
        venues_upper = get_venues_for_asset_groups(["CEFI"])
        venues_lower = get_venues_for_asset_groups(["cefi"])
        assert venues_upper == venues_lower


# ---------------------------------------------------------------------------
# UAC invariant tests — venue_core refactor (instrument_universe_registry_consolidation)
# ---------------------------------------------------------------------------


class TestVenueProducerUACInvariant:
    """Invariant tests asserting each AG's IS venue producer == the expected UAC-derived set.

    For CEFI: IS uses expand_cefi_tardis_endpoints(UAC["cefi"]) — the invariant
    asserts the round-trip.
    For TRADFI: IS uses UAC["tradfi"] minus YAHOO_FINANCE (named filter).
    For PREDICTION: IS uses UAC["prediction"] directly.
    For DEFI: NOW EQUAL — UAC VENUES_BY_ASSET_GROUP["defi"] was narrowed to the
      IS-producible set P (== _build_defi_venues(); @6bcff215, operator-approved defi
      MVP exclusion).  The drift-guard below asserts full set-equality (both directions),
      superseding the earlier subset-only guard.
    For SPORTS: EXEMPT — IS owns reference-data providers (API_FOOTBALL/FOOTYSTATS/
      etc.); UAC sports = market-data/odds venues (ODDS_API/PINNACLE/…) owned by
      MTDS.  Two orthogonal registries (Decision C, operator 2026-06-29); set-equality
      is intentionally NOT asserted.
    """

    def test_cefi_set_equals_expand_uac_cefi(self) -> None:
        """IS cefi venues == expand_cefi_tardis_endpoints(UAC cefi) — no drift."""
        from unified_api_contracts.registry.market_data_categories import VENUES_BY_ASSET_GROUP

        from instruments_service.engine.orchestrator import expand_cefi_tardis_endpoints

        expected = set(expand_cefi_tardis_endpoints(VENUES_BY_ASSET_GROUP["cefi"]))
        actual = set(get_venues_for_asset_groups(["CEFI"]))
        assert actual == expected, (
            f"CeFi venue producer diverges from UAC.\n"
            f"  Extra in IS (not in expand(UAC)): {actual - expected}\n"
            f"  Missing in IS (in expand(UAC)): {expected - actual}"
        )

    def test_cefi_includes_kalshi_perp_and_polymarket_perp(self) -> None:
        """KALSHI-PERP and POLYMARKET-PERP must be in the CeFi IS list (UAC cefi venues).

        These were previously omitted from _CEFI_VENUES (the bug fixed by this refactor).
        The expand function auto-includes them as passthrough because they appear in the
        UAC cefi list.
        """
        venues = set(get_venues_for_asset_groups(["CEFI"]))
        assert "KALSHI-PERP" in venues, "KALSHI-PERP must be in IS cefi (was silently omitted before)"
        assert "POLYMARKET-PERP" in venues, "POLYMARKET-PERP must be in IS cefi (was silently omitted before)"

    def test_tradfi_set_equals_uac_tradfi_minus_yahoo_finance(self) -> None:
        """IS tradfi venues == UAC tradfi minus YAHOO_FINANCE (named non-venue filter)."""
        from unified_api_contracts.registry.market_data_categories import VENUES_BY_ASSET_GROUP

        expected = set(VENUES_BY_ASSET_GROUP["tradfi"]) - {"YAHOO_FINANCE"}
        actual = set(get_venues_for_asset_groups(["TRADFI"]))
        assert actual == expected, (
            f"TradFi venue producer diverges from UAC minus YAHOO_FINANCE.\n"
            f"  Extra in IS: {actual - expected}\n"
            f"  Missing in IS: {expected - actual}"
        )

    def test_tradfi_excludes_yahoo_finance(self) -> None:
        """YAHOO_FINANCE must NOT appear in the IS tradfi enumeration list.

        It is a legacy source-as-venue artifact in UAC (not a real fetchable venue).
        The _TRADFI_NON_VENUE_KEYS filter excludes it explicitly.
        """
        venues = set(get_venues_for_asset_groups(["TRADFI"]))
        assert "YAHOO_FINANCE" not in venues, "YAHOO_FINANCE must be excluded (not a real venue)"

    def test_prediction_set_equals_uac_prediction(self) -> None:
        """IS prediction venues == UAC VENUES_BY_ASSET_GROUP["prediction"] exactly."""
        from unified_api_contracts.registry.market_data_categories import VENUES_BY_ASSET_GROUP

        expected = set(VENUES_BY_ASSET_GROUP["prediction"])
        actual = set(get_venues_for_asset_groups(["PREDICTION"]))
        assert actual == expected, (
            f"Prediction venue producer diverges from UAC.\n"
            f"  Extra in IS: {actual - expected}\n"
            f"  Missing in IS: {expected - actual}"
        )

    def test_defi_set_equals_uac_denominator_drift_guard(self) -> None:
        """DeFi denominator drift-guard: IS defi producer == UAC VENUES_BY_ASSET_GROUP["defi"].

        Post-@6bcff215 (operator-approved defi MVP exclusion) UAC
        VENUES_BY_ASSET_GROUP["defi"] was narrowed to the IS-producible set P
        (== _build_defi_venues(): 55 venues, no UAC-only tail). This upgrades the earlier
        subset-only guard to full set-equality, so a future change to EITHER side — IS
        adds/drops a producer, or UAC re-widens the defi denominator — that re-introduces
        denominator/producible drift fails CI.
        """
        from unified_api_contracts.registry.market_data_categories import VENUES_BY_ASSET_GROUP

        is_defi = set(get_venues_for_asset_groups(["DEFI"]))
        uac_defi = set(VENUES_BY_ASSET_GROUP["defi"])
        assert is_defi == uac_defi, (
            f"DeFi venue producer diverges from the UAC defi denominator (must stay == the "
            f"IS-producible set P after @6bcff215).\n"
            f"  Extra in IS (not in UAC): {is_defi - uac_defi}\n"
            f"  UAC-only (denominator re-widened): {uac_defi - is_defi}"
        )
        # single-producer invariant: the AG helper resolves to _build_defi_venues()
        assert is_defi == set(_build_defi_venues()), (
            "get_venues_for_asset_groups(['DEFI']) must == _build_defi_venues() "
            "(one producer; no second defi venue source)"
        )

    def test_sports_exempt_is_disjoint_from_uac_sports(self) -> None:
        """SPORTS is EXEMPT from set-equality (Decision C, operator 2026-06-29).

        IS sports = reference-data providers (API_FOOTBALL/FOOTYSTATS/UNDERSTAT/
        TRANSFERMARKT/SOCCER_FOOTBALL_INFO/OPEN_METEO).
        UAC sports = market-data/odds venues (ODDS_API/PINNACLE/BETFAIR*/DRAFTKINGS/
        FANDUEL) owned by MTDS.
        They are completely orthogonal sets (no overlap).
        """
        from unified_api_contracts.registry.market_data_categories import VENUES_BY_ASSET_GROUP

        is_sports = set(get_venues_for_asset_groups(["SPORTS"]))
        uac_sports = set(VENUES_BY_ASSET_GROUP["sports"])
        overlap = is_sports & uac_sports
        assert not overlap, (
            f"IS sports and UAC sports must be disjoint (two-registry model): overlap={overlap}"
        )


# ---------------------------------------------------------------------------
# is_venue_available
# ---------------------------------------------------------------------------


class TestIsVenueAvailable:
    def test_unknown_venue_returns_true(self) -> None:
        assert is_venue_available("TOTALLY_UNKNOWN_VENUE", "2020-01-01") is True

    def test_known_venue_before_launch_returns_false(self) -> None:
        # If a venue has a launch date in 2020, checking 2010 should be False.
        # SSOT: UAC VenueMapping.get_instrument_discovery_start (replaced the
        # local _VENUE_LAUNCH_DATES dict in commit a3355f2).
        from instruments_service.engine.orchestrator import _VENUE_MAPPING

        for venue in ("BINANCE-SPOT", "DERIBIT", "HYPERLIQUID", "BYBIT", "OKX"):
            launch_date = _VENUE_MAPPING.get_instrument_discovery_start(venue)
            if launch_date is not None and launch_date > "2010-01-01":
                assert is_venue_available(venue, "2010-01-01") is False
                return
        # If no known venue resolved (unexpected), fail loud - UAC drift signal.
        raise AssertionError(
            "No probed venue resolved a discovery-start date via VenueMapping - "
            "either UAC venue_start_dates regressed or every probed venue launched pre-2010."
        )

    def test_known_venue_after_launch_returns_true(self) -> None:
        # SSOT: UAC VenueMapping.get_instrument_discovery_start.
        from instruments_service.engine.orchestrator import _VENUE_MAPPING

        for venue in ("BINANCE-SPOT", "DERIBIT", "HYPERLIQUID", "BYBIT", "OKX"):
            if _VENUE_MAPPING.get_instrument_discovery_start(venue) is not None:
                assert is_venue_available(venue, "2030-01-01") is True
                return
        raise AssertionError("No probed venue resolved a discovery-start date via VenueMapping - UAC drift.")


# ---------------------------------------------------------------------------
# filter_instruments_by_date
# ---------------------------------------------------------------------------


class TestFilterInstrumentsByDate:
    def test_no_date_constraints_passes(self) -> None:
        record = _make_record()
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_since_before_date_passes(self) -> None:
        record = _make_record(available_since=datetime(2024, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_since_after_date_filtered(self) -> None:
        record = _make_record(available_since=datetime(2025, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 0

    def test_available_to_after_date_passes(self) -> None:
        record = _make_record(available_to=datetime(2025, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_to_before_date_filtered(self) -> None:
        record = _make_record(available_to=datetime(2024, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 0

    def test_defi_venue_missing_available_since_warns(self, caplog) -> None:
        record = _make_record(venue="AAVE_V3-ETHEREUM")
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        defi_venues = frozenset(["AAVE_V3-ETHEREUM"])
        with caplog.at_level("WARNING"):
            result = filter_instruments_by_date([record], date_dt, defi_venues=defi_venues)
        assert len(result) == 1  # still included
        assert any("available_from_datetime=None" in r.message for r in caplog.records)

    def test_multiple_records_filters_correctly(self) -> None:
        r1 = _make_record(instrument_key="A", available_since=datetime(2024, 1, 1, tzinfo=UTC))
        r2 = _make_record(instrument_key="B", available_since=datetime(2025, 1, 1, tzinfo=UTC))
        r3 = _make_record(instrument_key="C")
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([r1, r2, r3], date_dt)
        assert len(result) == 2  # r1 and r3 pass, r2 filtered


# ---------------------------------------------------------------------------
# filter_defi_instruments_by_relevance
# ---------------------------------------------------------------------------


class TestFilterDefiInstrumentsByRelevance:
    def test_dex_both_major_passes(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-ETHEREUM",
            base_asset="WETH",
            quote_asset="USDC",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_dex_one_long_tail_filtered(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-ETHEREUM",
            base_asset="PEPE",
            quote_asset="WETH",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 0

    def test_lending_base_major_passes(self) -> None:
        record = _make_record(
            venue="AAVE_V3-ETHEREUM",
            base_asset="WETH",
            quote_asset="USD",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_lending_base_long_tail_filtered(self) -> None:
        record = _make_record(
            venue="AAVE_V3-ETHEREUM",
            base_asset="SHIB",
            quote_asset="USD",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 0


# ---------------------------------------------------------------------------
# _build_defi_venues
# ---------------------------------------------------------------------------


class TestBuildDefiVenues:
    def test_includes_static_venues(self) -> None:
        venues = _build_defi_venues()
        assert "LIDO-ETHEREUM" in venues
        assert "ETHERFI-ETHEREUM" in venues
        assert "ETHENA-ETHEREUM" in venues

    def test_includes_solana_venues(self) -> None:
        venues = _build_defi_venues()
        assert "DRIFT-SOLANA" in venues
        assert "KAMINO-SOLANA" in venues
        assert "RAYDIUM-SOLANA" in venues
        assert "ORCA-SOLANA" in venues
        assert "MARINADE-SOLANA" in venues

    def test_includes_subgraph_venues(self) -> None:
        venues = _build_defi_venues()
        assert any("AAVE_V3" in v for v in venues)
        assert any("UNISWAP_V3" in v for v in venues)

    def test_returns_non_empty(self) -> None:
        venues = _build_defi_venues()
        assert len(venues) > 10


# ---------------------------------------------------------------------------
# _get_instruments_bucket
# ---------------------------------------------------------------------------


class TestGetInstrumentsBucket:
    def test_bucket_with_category(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.resolve_bucket_name",
            return_value="instruments-store-defi-prd-test-project",
        ):
            bucket = _get_instruments_bucket("DEFI")
        assert "instruments" in bucket.lower()

    def test_bucket_resolve_called_with_asset_group(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.resolve_bucket_name",
            return_value="instruments-store-cefi-prd-test-project",
        ) as mock_resolve:
            bucket = _get_instruments_bucket("CEFI")
        call_kwargs = mock_resolve.call_args.kwargs
        assert call_kwargs.get("asset_group") == "cefi"
        assert "instruments" in bucket.lower()

    def test_bucket_test_mode(self) -> None:
        import instruments_service.config.service_config as sc

        old = sc._config
        try:
            sc._config = None
            with patch.dict("os.environ", {"IS_TEST_RUN": "true"}):
                sc._config = None
                cfg = sc.InstrumentsServiceConfig()
                cfg.is_test_run = True
                sc._config = cfg
                with patch(
                    "instruments_service.engine.orchestrator.resolve_bucket_name",
                    return_value="instruments-store-defi-test-test-project",
                ):
                    bucket = _get_instruments_bucket("DEFI")
                assert "-test-" in bucket
        finally:
            sc._config = old


# ---------------------------------------------------------------------------
# _write_catalogue_record
# ---------------------------------------------------------------------------


class TestWriteCatalogueRecord:
    def test_non_blocking_on_error(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.ManifestWriter",
            side_effect=ConnectionError("no GCS"),
        ):
            # Should not raise
            _write_catalogue_record(
                "bucket",
                "instrument_availability/by_date/day=2026-03-22/venue=BINANCE-SPOT/instruments.parquet",
                "2026-03-22",
                10,
            )

    def test_successful_write(self) -> None:
        mock_writer = MagicMock()
        with patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_writer):
            _write_catalogue_record(
                "bucket",
                "instrument_availability/by_date/day=2026-03-22/venue=BINANCE-SPOT/instruments.parquet",
                "2026-03-22",
                10,
            )
        mock_writer.record_captured_from_counts.assert_called_once()
        mock_writer.write.assert_called_once()


# ---------------------------------------------------------------------------
# _write_fixture_mapping
# ---------------------------------------------------------------------------


class TestWriteFixtureMapping:
    def test_404_forward_poll_window_no_classify(self) -> None:
        """Missing instruments parquet on a future date in the rolling window is expected (zero fixtures)."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = Exception(
            "404 GET https://storage.googleapis.com/... No such object: .../instruments.parquet"
        )
        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.datetime") as mock_datetime,
            patch("instruments_service.engine.orchestrator.classify_and_emit_error") as mock_classify,
        ):
            mock_datetime.now.return_value = datetime(2026, 4, 21, 12, 0, 0, tzinfo=UTC)
            _write_fixture_mapping("test-bucket", "2026-04-28")
        mock_classify.assert_not_called()

    def test_404_historical_date_no_classify(self) -> None:
        """Missing instruments parquet on a HISTORICAL forward-polled date is also benign.

        Forward-polled days that were captured via enrichment-only mode never wrote
        instrument_availability/.../instruments.parquet at fixture-poll time. The
        downstream fixture_mapping is best-effort; absent the upstream parquet
        nothing to map. Must silently skip (no classify_and_emit_error) regardless
        of date. Reference: VM af-backfill-20260513-161517 failure 2026-05-13 +
        issue doc api_football_enrichment_preflight_runtime_mismatch_2026_05_13.md.
        """
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = Exception(
            "404 GET https://storage.googleapis.com/... No such object: .../instruments.parquet"
        )
        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.datetime") as mock_datetime,
            patch("instruments_service.engine.orchestrator.classify_and_emit_error") as mock_classify,
        ):
            mock_datetime.now.return_value = datetime(2026, 5, 13, 12, 0, 0, tzinfo=UTC)
            # 2026-04-26 is 17 days HISTORICAL relative to today (was failing before fix)
            _write_fixture_mapping("test-bucket", "2026-04-26")
        mock_classify.assert_not_called()


# ---------------------------------------------------------------------------
# Bug 2 regression - UnboundLocalError on get_leagues_needing_refresh
# ---------------------------------------------------------------------------


class TestGetLeaguesNeedingRefreshImportScope:
    """Regression for Bug 2 - forward-poll VM ``af-backfill-20260421-142640``.

    A conditional ``from unified_api_contracts.sports import
    get_leagues_needing_refresh`` inside the TRANSFERMARKT branch of
    ``process_instruments`` made Python treat the name as a function-local,
    so the second call site (zero-fixture fast path) raised
    ``UnboundLocalError: cannot access local variable
    'get_leagues_needing_refresh' where it is not associated with a value``
    whenever the code path skipped the TRANSFERMARKT branch.

    Fix: hoist the import to module scope (single source of truth at the
    module top-level) and drop the local ``from`` statement. These tests
    lock the fix in so no one re-adds a conditional import.
    """

    def test_imported_at_module_level(self) -> None:
        """The symbol must be resolvable on the orchestrator module namespace."""
        from instruments_service.engine import orchestrator

        assert hasattr(orchestrator, "get_leagues_needing_refresh"), (
            "get_leagues_needing_refresh must be imported at module scope - "
            "a local import would re-introduce Bug 2 (UnboundLocalError)."
        )
        assert callable(orchestrator.get_leagues_needing_refresh)

    def test_no_local_import_in_source(self) -> None:
        """Source scan: no conditional ``import get_leagues_needing_refresh`` remains inside any function body.

        Prevents regression: the pattern ``from ... import
        get_leagues_needing_refresh`` inside ``process_instruments`` or
        any helper would silently shadow the module-level name and trigger
        UnboundLocalError on alternative code paths.
        """
        src = _orchestrator_package_source()
        # Strip the single authoritative module-level import block before scanning.
        # The module-level import lives in a ``from unified_api_contracts.sports import (`` block.
        lines = src.splitlines()
        in_module_import_block = False
        filtered_lines: list[str] = []
        for line in lines:
            if line.startswith("from unified_api_contracts.sports import ("):
                in_module_import_block = True
                continue
            if in_module_import_block:
                if line.strip() == ")":
                    in_module_import_block = False
                continue
            filtered_lines.append(line)
        filtered_src = "\n".join(filtered_lines)
        assert "import get_leagues_needing_refresh" not in filtered_src, (
            "No function-local import of get_leagues_needing_refresh is "
            "permitted - it caused Bug 2 UnboundLocalError. Keep the "
            "symbol imported once at module scope."
        )


# ---------------------------------------------------------------------------
# Bug 3 regression - recovery_fixture_ids ignored by zero-fixture fast paths
# ---------------------------------------------------------------------------


class TestRecoveryFixtureIdsBypassBug:
    """Regression for recovery_fixture_ids bypass bug (2026-05-14).

    Two fast paths inside process_instruments ignored --recovery-fixture-ids:
    1. Per-fixture _skip_urdi path: early-exited with ``return {}`` when
       _read_fixture_ids_from_gcs returned empty, before checking recovery_fixture_ids.
    2. Zero-fixture path: passed fixture_ids_override=[] unconditionally, meaning
       recovery_fixture_ids was never used even when provided.

    Both paths now guard with ``if not recovery_fixture_ids`` before early-exit or
    empty-list override. Source scans lock in the fix pattern.

    Reference: VM af-backfill-20260514-102928 (Phase 3.C, Man City vs Crystal Palace
    EPL FT, fixture 1379275) completed in ~22s with no FIXTURE_STATS written.
    Issue doc: plans/active/issues/orchestrator_zero_fixture_path_recovery_bypass_bug_2026_05_14.md.
    """

    def test_skip_urdi_early_exit_guards_recovery_fixture_ids(self) -> None:
        """_skip_urdi path must check recovery_fixture_ids before early-exit.

        Pattern: ``if not gcs_fixture_ids and not recovery_fixture_ids:``
        must appear before ``return {}``. If this guard is missing, a VM launched
        with ``--entity FIXTURE_STATS --recovery-fixture-ids <parquet>`` on a date
        where GCS has no completed fixtures will exit in ~22s with zero data written.
        """
        src = _orchestrator_package_source()
        assert "not gcs_fixture_ids and not recovery_fixture_ids" in src, (
            "The _skip_urdi early-exit must guard against recovery_fixture_ids. "
            "Pattern 'not gcs_fixture_ids and not recovery_fixture_ids' missing - "
            "restores Bug 3 bypass (recovery mode ignored when GCS fixtures empty)."
        )

    def test_zero_fixture_path_uses_recovery_ids_when_provided(self) -> None:
        """Zero-fixture path must use recovery_fixture_ids as fixture_ids_override.

        Pattern: ``list(recovery_fixture_ids) if recovery_fixture_ids else []``
        must appear in fixture_ids_override kwarg of _fetch_sports_reference_data.
        If this guard is missing, zero-fixture dates with --recovery-fixture-ids
        silently skip all per-fixture entity fetches.
        """
        src = _orchestrator_package_source()
        assert "list(recovery_fixture_ids) if recovery_fixture_ids else []" in src, (
            "Zero-fixture path must use recovery_fixture_ids as fixture_ids_override. "
            "Pattern 'list(recovery_fixture_ids) if recovery_fixture_ids else []' missing - "
            "restores Bug 3 bypass (recovery IDs ignored on zero-fixture dates)."
        )


class TestWriteVenueCanonicalPartition:
    """Bug 5: the parquet ``venue=`` partition must be the canonical DeFi venue
    (``AAVE_V3-ARBITRUM``), not the glued caller form (``AAVEV3-ARBITRUM``), so it
    matches the canonical manifest venue and deployment-ui pool-breakdown can
    resolve the parquet. SSOT:
    plans/active/issues/defi_coverage_capability_alignment_2026_05_22.md Bug 5.
    """

    def _run(self, venue_in: str) -> dict[str, object]:
        import pandas as pd

        captured: dict[str, object] = {}

        def _capture(sink, data, partition, filename, venue, entity):
            captured["partition"] = partition
            captured["venue"] = venue

        sampler = MagicMock()
        sampler.enable_sampling = False
        df = pd.DataFrame([{"instrument_id": "x", "venue": venue_in}])
        with (
            patch("instruments_service.engine.orchestrator._gated_sink_write", side_effect=_capture),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda d, when: d,
            ),
        ):
            _write_venue(venue_in, df, "2026-05-03", "bkt", MagicMock(), {}, sampler, manifest=None)
        return captured

    def test_glued_defi_venue_partition_canonicalized(self) -> None:
        captured = self._run("AAVEV3-ARBITRUM")
        assert captured["partition"]["venue"] == "AAVE_V3-ARBITRUM"  # type: ignore[index]
        assert captured["venue"] == "AAVE_V3-ARBITRUM"

    def test_already_canonical_defi_venue_unchanged(self) -> None:
        captured = self._run("AAVE_V3-ARBITRUM")
        assert captured["partition"]["venue"] == "AAVE_V3-ARBITRUM"  # type: ignore[index]

    def test_glued_uniswap_v3_canonicalized(self) -> None:
        captured = self._run("UNISWAPV3-ETHEREUM")
        assert captured["partition"]["venue"] == "UNISWAP_V3-ETHEREUM"  # type: ignore[index]

    def test_non_defi_venue_passes_through(self) -> None:
        # CeFi venue (no DeFi chain) must NOT be rewritten.
        captured = self._run("BINANCE")
        assert captured["partition"]["venue"] == "BINANCE"  # type: ignore[index]


class TestWriteVenueDataTypeInstrumentsStamp:
    """Regression: non-sports ``_write_venue`` MUST stamp ``data_type='instruments'``.

    Historical bug (2026-06-29..2026-07-06): the writer emitted ``data_type=""`` for
    every cefi/tradfi/defi captured row. Downstream consumers using the canonical
    honest-coverage filter ``capture_status='captured' AND data_type='instruments'``
    (matching ``REFERENCE_DATA_TYPE`` in ``scripts/migrate_instruments_store_v9.py``)
    silently missed 260 cefi shards (26 venues x 10 days). This test guards the
    stamp at emission time so a future regression can't reintroduce the drift.
    Issue: ``plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md``.
    """

    def _run(self, venue_in: str) -> MagicMock:
        import pandas as pd

        sampler = MagicMock()
        sampler.enable_sampling = False
        df = pd.DataFrame([{"instrument_id": "x", "venue": venue_in, "instrument_type": "PERPETUAL"}])
        manifest = MagicMock()
        with (
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda d, when: d,
            ),
        ):
            _write_venue(venue_in, df, "2026-07-06", "bkt", MagicMock(), {}, sampler, manifest=manifest)
        return manifest

    def test_cefi_captured_row_stamps_data_type_instruments(self) -> None:
        """A fresh cefi captured shard lands with data_type='instruments' (not blank)."""
        manifest = self._run("BINANCE-SPOT")
        manifest.record_captured.assert_called_once()
        kwargs = manifest.record_captured.call_args.kwargs
        assert kwargs["data_type"] == "instruments", (
            "Non-sports writer must stamp data_type='instruments' at emission time "
            f"(matches REFERENCE_DATA_TYPE); got {kwargs['data_type']!r}"
        )
        assert kwargs["asset_group"] == "cefi"

    def test_cefi_on_chain_venue_stamps_data_type_instruments(self) -> None:
        """On-chain cefi venue (EXTENDED-STARKNET) lands with data_type='instruments', chain=''."""
        manifest = self._run("EXTENDED-STARKNET")
        manifest.record_captured.assert_called_once()
        kwargs = manifest.record_captured.call_args.kwargs
        assert kwargs["data_type"] == "instruments"
        assert kwargs["asset_group"] == "cefi"
        assert kwargs["chain"] == ""

    def test_defi_captured_row_stamps_data_type_instruments(self) -> None:
        """DeFi venue (AAVE_V3-ETHEREUM) also lands with data_type='instruments'."""
        manifest = self._run("AAVE_V3-ETHEREUM")
        manifest.record_captured.assert_called_once()
        kwargs = manifest.record_captured.call_args.kwargs
        assert kwargs["data_type"] == "instruments"
        assert kwargs["asset_group"] == "defi"


# ---------------------------------------------------------------------------
# _validate_predictions_null_rates
# ---------------------------------------------------------------------------


class TestValidatePredictionsNullRates:
    """Tests for _validate_predictions_null_rates (lines 5773-5821)."""

    def test_empty_df_returns_no_violations(self) -> None:
        df = pd.DataFrame(columns=["fixture_id", "source", "kickoff_utc"])
        result = _validate_predictions_null_rates(df, "2026-01-15")
        assert result == []

    def test_all_core_cols_present_and_filled_no_violations(self) -> None:
        df = pd.DataFrame(
            {
                "fixture_id": ["f1", "f2"],
                "source": ["fs", "fs"],
                "kickoff_utc": ["2026-01-15T15:00:00Z", "2026-01-15T17:00:00Z"],
                "home_team": ["Arsenal", "Chelsea"],
                "away_team": ["Man City", "Liverpool"],
            }
        )
        result = _validate_predictions_null_rates(df, "2026-01-15")
        assert result == []

    def test_missing_core_column_adds_violation(self) -> None:
        df = pd.DataFrame(
            {
                "source": ["fs", "fs"],
                "kickoff_utc": ["2026-01-15T15:00:00Z", "2026-01-15T17:00:00Z"],
                "home_team": ["Arsenal", "Chelsea"],
                "away_team": ["Man City", "Liverpool"],
            }
        )
        violations = _validate_predictions_null_rates(df, "2026-01-15")
        assert any("fixture_id missing from schema" in v for v in violations)

    def test_high_null_rate_in_core_col_adds_violation(self) -> None:
        # 2 out of 2 rows have null fixture_id → 100% null > 5% threshold
        df = pd.DataFrame(
            {
                "fixture_id": [None, None],
                "source": ["fs", "fs"],
                "kickoff_utc": ["2026-01-15T15:00:00Z", "2026-01-15T17:00:00Z"],
                "home_team": ["Arsenal", "Chelsea"],
                "away_team": ["Man City", "Liverpool"],
            }
        )
        violations = _validate_predictions_null_rates(df, "2026-01-15")
        assert any("fixture_id" in v and "null rate" in v for v in violations)

    def test_potential_col_null_rate_exceeds_threshold_adds_violation(self) -> None:
        # btts_potential: 3 nulls out of 3 rows → 100% > 20%
        df = pd.DataFrame(
            {
                "fixture_id": ["f1", "f2", "f3"],
                "source": ["fs", "fs", "fs"],
                "kickoff_utc": ["2026-01-15T15:00:00Z"] * 3,
                "home_team": ["A", "B", "C"],
                "away_team": ["D", "E", "F"],
                "btts_potential": [None, None, None],
            }
        )
        violations = _validate_predictions_null_rates(df, "2026-01-15")
        assert any("btts_potential" in v for v in violations)

    def test_potential_col_absent_no_violation(self) -> None:
        # Missing potential columns are skipped (optional)
        df = pd.DataFrame(
            {
                "fixture_id": ["f1"],
                "source": ["fs"],
                "kickoff_utc": ["2026-01-15T15:00:00Z"],
                "home_team": ["Arsenal"],
                "away_team": ["Man City"],
            }
        )
        violations = _validate_predictions_null_rates(df, "2026-01-15")
        assert violations == []


# ---------------------------------------------------------------------------
# _load_venue_coordinates
# ---------------------------------------------------------------------------


class TestLoadVenueCoordinates:
    """Tests for _load_venue_coordinates (lines 7409-7441)."""

    def test_blob_not_found_returns_empty(self) -> None:
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _load_venue_coordinates("test-bucket")
        assert result == {}

    def test_parquet_missing_venue_id_column_returns_empty(self, tmp_path) -> None:
        df = pd.DataFrame({"latitude": [53.43], "longitude": [-2.96]})
        local_path = str(tmp_path / "venues.parquet")
        df.to_parquet(local_path)

        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _load_venue_coordinates("test-bucket")
        assert result == {}

    def test_parquet_missing_lat_lon_columns_returns_empty(self, tmp_path) -> None:
        df = pd.DataFrame({"venue_id": ["v1"]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _load_venue_coordinates("test-bucket")
        assert result == {}

    def test_valid_parquet_returns_coords(self, tmp_path) -> None:
        df = pd.DataFrame(
            {
                "venue_id": ["v1", "v2"],
                "latitude": [53.43, 51.5],
                "longitude": [-2.96, -0.12],
            }
        )
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _load_venue_coordinates("test-bucket")
        assert "v1" in result
        assert result["v1"] == (53.43, -2.96)
        assert "v2" in result

    def test_zero_coords_skipped(self, tmp_path) -> None:
        df = pd.DataFrame(
            {
                "venue_id": ["v_zero"],
                "latitude": [0.0],
                "longitude": [0.0],
            }
        )
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _load_venue_coordinates("test-bucket")
        assert "v_zero" not in result

    def test_gcs_exception_returns_empty(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            side_effect=RuntimeError("GCS down"),
        ):
            result = _load_venue_coordinates("test-bucket")
        assert result == {}


# ---------------------------------------------------------------------------
# _extract_fixture_venue_ids
# ---------------------------------------------------------------------------


class TestExtractFixtureVenueIds:
    """Tests for _extract_fixture_venue_ids (lines 7444-7479)."""

    def test_no_blob_returns_empty(self) -> None:
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == []

    def test_venue_dict_ids_extracted(self, tmp_path) -> None:
        df = pd.DataFrame({"venue": [{"venue_id": "V1"}, {"venue_id": "V2"}]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == ["V1", "V2"]

    def test_venue_string_ids_extracted(self, tmp_path) -> None:
        df = pd.DataFrame({"venue": ["ANFIELD", "OLD_TRAFFORD"]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == ["ANFIELD", "OLD_TRAFFORD"]

    def test_duplicate_venue_ids_deduplicated(self, tmp_path) -> None:
        df = pd.DataFrame({"venue": ["ANFIELD", "ANFIELD", "OLD_TRAFFORD"]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == ["ANFIELD", "OLD_TRAFFORD"]

    def test_no_venue_column_returns_empty(self, tmp_path) -> None:
        df = pd.DataFrame({"fixture_id": ["f1", "f2"]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == []

    def test_gcs_exception_returns_empty(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            side_effect=RuntimeError("GCS down"),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == []

    def test_venue_dict_without_venue_id_key_skipped(self, tmp_path) -> None:
        df = pd.DataFrame({"venue": [{"other_key": "xyz"}, {"venue_id": "V1"}]})
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = lambda path: df.to_parquet(path)
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.tempfile.gettempdir", return_value=str(tmp_path)),
        ):
            result = _extract_fixture_venue_ids("test-bucket", "2026-01-15")
        assert result == ["V1"]


def test_derive_instrument_type_single_type_stamps_real_type() -> None:
    """Audit §K: a single-type venue df → the real instrument_type is stamped."""
    from instruments_service.engine.orchestrator import _derive_instrument_type

    df = pd.DataFrame(
        [
            {"instrument_key": "BTC-PERP", "instrument_type": "PERPETUAL"},
            {"instrument_key": "ETH-PERP", "instrument_type": "PERPETUAL"},
        ]
    )
    assert _derive_instrument_type(df) == "PERPETUAL"


def test_derive_instrument_type_mixed_types_blank() -> None:
    """A mixed-type venue df → "" (a single tag would misrepresent the shard)."""
    from instruments_service.engine.orchestrator import _derive_instrument_type

    df = pd.DataFrame(
        [
            {"instrument_key": "BTC-PERP", "instrument_type": "PERPETUAL"},
            {"instrument_key": "BTC-USDT", "instrument_type": "SPOT_PAIR"},
        ]
    )
    assert _derive_instrument_type(df) == ""


def test_derive_instrument_type_absent_or_empty_blank() -> None:
    """No instrument_type column, an empty df, or all-blank values → "" (honest blank)."""
    from instruments_service.engine.orchestrator import _derive_instrument_type

    assert _derive_instrument_type(pd.DataFrame([{"instrument_key": "X"}])) == ""
    assert _derive_instrument_type(pd.DataFrame(columns=["instrument_type"])) == ""
    assert _derive_instrument_type(pd.DataFrame([{"instrument_type": ""}, {"instrument_type": None}])) == ""
