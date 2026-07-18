"""process_write._records_to_dataframe fixture-match column materialisation (A4).

Covers the join added to
``instruments_service/engine/orchestrator/process_write.py::_records_to_dataframe``
that materialises the six additive prediction fixture-match attributes
(``af_league_id``, ``home_team_canonical_id``, ``away_team_canonical_id``,
``fixture_date``, ``af_fixture_id``, ``af_fixture_match_status``) from the
per-instrument side-table onto the availability parquet — mirroring the
``clob_token_ids`` join.

Asserts:
* a prediction soccer record with a MATCHED side-table entry serialises all six
  columns with the CONTRACT types (``af_league_id`` → ``str``, ``fixture_date`` →
  ``date``, ``af_fixture_id`` → ``int``, statuses/team ids → ``str``);
* the int→str / str→date conversions (incl. bad-value → ``None`` guard) at the
  type boundary;
* a non-prediction (cefi/tradfi/defi) record round-trips UNCHANGED with all six
  columns ``None`` and no crash (RULE 11 — every asset group flows this path).

Credential-free, no cloud/network: the side-table is a plain in-process dict.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from unified_api_contracts.internal import InstrumentRecord, InstrumentType

from instruments_service.engine.orchestrator.process_write import (
    _fixture_date_to_date,
    _records_to_dataframe,
)
from instruments_service.reference_data.adapters.prediction.fixture_match import (
    FixtureMatchAttributes,
    FixtureMatchStatus,
    register_fixture_match,
    reset_fixture_match_registry,
)

_FIXTURE_MATCH_COLUMNS = (
    "af_league_id",
    "home_team_canonical_id",
    "away_team_canonical_id",
    "fixture_date",
    "af_fixture_id",
    "af_fixture_match_status",
)


def _prediction_record(instrument_key: str) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue="POLYMARKET",
        instrument_type=InstrumentType.PREDICTION_MARKET,
        raw_symbol="soccer-mkt",
    )


def _cefi_spot_record(instrument_key: str = "BINANCE:SPOT_PAIR:ETHUSDT") -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue="BINANCE",
        instrument_type=InstrumentType.SPOT_PAIR,
        base_asset="ETH",
        quote_asset="USDT",
    )


class TestFixtureDateToDate:
    """The str→date boundary converter is honest-absence (never raises)."""

    def test_valid_iso_string_converts(self) -> None:
        assert _fixture_date_to_date("2026-03-22") == date(2026, 3, 22)

    def test_none_returns_none(self) -> None:
        assert _fixture_date_to_date(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _fixture_date_to_date("") is None

    def test_bad_value_returns_none_not_raises(self) -> None:
        assert _fixture_date_to_date("not-a-date") is None
        assert _fixture_date_to_date("2026-13-99") is None


class TestFixtureMatchColumnJoin:
    def setup_method(self) -> None:
        reset_fixture_match_registry()

    def teardown_method(self) -> None:
        reset_fixture_match_registry()

    def test_matched_soccer_market_serialises_all_six_columns(self) -> None:
        ik = "POLYMARKET:PREDICTION_MARKET:0xsoccer_matched"
        register_fixture_match(
            ik,
            FixtureMatchAttributes(
                af_league_id=39,  # int in the side-table
                home_team_canonical_id="liverpool",
                away_team_canonical_id="brentford",
                fixture_date="2026-03-22",  # str in the side-table
                af_fixture_id=123456,
                af_fixture_match_status=FixtureMatchStatus.MATCHED,
            ),
        )

        df = _records_to_dataframe([_prediction_record(ik)])
        row = df.iloc[0]

        # Values + CONTRACT types (int→str, str→date; int/str stay).
        assert row["af_league_id"] == "39"
        assert isinstance(row["af_league_id"], str)
        assert row["home_team_canonical_id"] == "liverpool"
        assert row["away_team_canonical_id"] == "brentford"
        assert row["fixture_date"] == date(2026, 3, 22)
        assert isinstance(row["fixture_date"], date)
        assert row["af_fixture_id"] == 123456
        assert int(row["af_fixture_id"]) == 123456
        assert row["af_fixture_match_status"] == FixtureMatchStatus.MATCHED

    def test_unresolved_entry_keeps_int_none_and_bad_date_honest(self) -> None:
        """af_league_id None passes through as None; a non-ISO fixture_date in the
        side-table degrades to None rather than crashing the write path."""
        ik = "POLYMARKET:PREDICTION_MARKET:0xsoccer_unresolved"
        register_fixture_match(
            ik,
            FixtureMatchAttributes(
                af_league_id=None,
                home_team_canonical_id=None,
                away_team_canonical_id=None,
                fixture_date="garbage-date",
                af_fixture_id=None,
                af_fixture_match_status=FixtureMatchStatus.UNRESOLVED_TEAM_NAME,
            ),
        )

        df = _records_to_dataframe([_prediction_record(ik)])
        row = df.iloc[0]

        assert row["af_league_id"] is None
        assert row["fixture_date"] is None
        assert row["af_fixture_id"] is None
        assert row["af_fixture_match_status"] == FixtureMatchStatus.UNRESOLVED_TEAM_NAME

    def test_prediction_record_without_side_table_entry_stays_none(self) -> None:
        ik = "POLYMARKET:PREDICTION_MARKET:0xnot_soccer"
        df = _records_to_dataframe([_prediction_record(ik)])
        row = df.iloc[0]
        for col in _FIXTURE_MATCH_COLUMNS:
            assert row[col] is None, f"{col} should be None with no side-table entry"

    def test_non_prediction_record_round_trips_unchanged(self) -> None:
        """RULE 11: a cefi instrument flows the SAME _records_to_dataframe path;
        the six new columns are present-and-None, and the record's own fields
        survive unchanged (additive, no crash)."""
        # A stray same-key registration must not bleed into a non-prediction row —
        # the join only fires on a matching instrument_key.
        register_fixture_match(
            "SOME:OTHER:KEY",
            FixtureMatchAttributes(
                af_league_id=39,
                home_team_canonical_id="x",
                away_team_canonical_id="y",
                fixture_date="2026-03-22",
                af_fixture_id=1,
                af_fixture_match_status=FixtureMatchStatus.MATCHED,
            ),
        )

        rec = _cefi_spot_record()
        df = _records_to_dataframe([rec])
        row = df.iloc[0]

        for col in _FIXTURE_MATCH_COLUMNS:
            assert row[col] is None, f"{col} must be None for a non-prediction row"
        # Own fields survive.
        assert row["instrument_key"] == rec.instrument_key
        assert row["base_asset"] == "ETH"
        assert row["quote_asset"] == "USDT"
        assert row["instrument_type"] == InstrumentType.SPOT_PAIR.value

    def test_mixed_batch_only_matched_row_populated(self) -> None:
        """A single _records_to_dataframe call carrying a matched prediction row +
        a cefi row: exactly the prediction row gets the columns populated."""
        ik = "POLYMARKET:PREDICTION_MARKET:0xsoccer_mixed"
        register_fixture_match(
            ik,
            FixtureMatchAttributes(
                af_league_id=140,
                home_team_canonical_id="real_madrid",
                away_team_canonical_id="barcelona",
                fixture_date="2026-04-01",
                af_fixture_id=999,
                af_fixture_match_status=FixtureMatchStatus.MATCHED,
            ),
        )

        df = _records_to_dataframe([_cefi_spot_record(), _prediction_record(ik)])
        cefi_row = df[df["instrument_key"] == "BINANCE:SPOT_PAIR:ETHUSDT"].iloc[0]
        pred_row = df[df["instrument_key"] == ik].iloc[0]

        # af_fixture_id mixes int (999) + None in one column → pandas may coerce to
        # float64 (999.0 / NaN); assert on absence-vs-value, not identity.
        assert pd.isna(cefi_row["af_fixture_id"])
        assert cefi_row["af_league_id"] is None
        assert pred_row["af_fixture_id"] == 999
        assert pred_row["af_league_id"] == "140"
        assert pred_row["fixture_date"] == date(2026, 4, 1)
