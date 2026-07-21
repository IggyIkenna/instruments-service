"""Guards the PREDICTION Phase-D adaptation of instruments-service
``scripts/pipeline_e2e_check.py`` (``prediction_consolidated_closeout_2026_07_18.md``
Phase-D):

* (A) the ``canonical`` regression cell (``_run_prediction_canonical_cell`` /
  ``_assert_prediction_records_canonical``) over the freshly-written instruments parquet —
  ``instrument_type == PREDICTION_MARKET``, canonical ``canonical_instrument_id``, and (soccer)
  a closed-set ``af_fixture_match_status``.
* (B) the CQG cluster + ``market_lifecycle`` grain cells (``_run_prediction_grain_cells``) —
  the two IS-PRODUCED reference grains the ``(asset_group, venue)`` shard atom collapses away,
  smoke-tested here (their producer) as distinct force/skip cells.
* RULE 11 — the prediction branch must NOT change cefi/tradfi/defi/sports: per-AG dedup'd
  shard-target counts are pinned, ``canonical`` is not in the default legs, and the canonical
  cell records ``skipped`` for every non-prediction cell.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS))


def _load():
    spec = importlib.util.spec_from_file_location("is_pe2e_prediction_mod", _SCRIPTS / "pipeline_e2e_check.py")
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    sys.modules["is_pe2e_prediction_mod"] = m
    spec.loader.exec_module(m)
    return m


@dataclass
class _FakeCell:
    asset_group: str
    venue: str
    sports_provider: str | None = None


# ---------------------------------------------------------------------------
# (A) _assert_prediction_records_canonical
# ---------------------------------------------------------------------------


def test_assert_records_all_canonical_including_soccer() -> None:
    m = _load()
    ids = ["POLYMARKET:PREDICTION_MARKET:0xabc", "KALSHI:PREDICTION_MARKET:KXEPL-25"]
    types = ["PREDICTION_MARKET", "PREDICTION_MARKET"]
    statuses = [None, "MATCHED"]  # 2nd is a soccer row with a valid closed-set status
    checked, canonical, violations = m._assert_prediction_records_canonical(ids, types, statuses)
    assert (checked, canonical) == (2, 2)
    assert violations == []


def test_assert_records_flags_lowercase_leakage_and_bad_fixture_status() -> None:
    m = _load()
    ids = ["POLYMARKET:PREDICTION_MARKET:0xa", "POLYMARKET:PREDICTION_MARKET:0xb", "POLYMARKET:PREDICTION_MARKET:0xc"]
    types = ["prediction_market", "BTC", "PREDICTION_MARKET"]  # lowercase dupe, underlying-leakage, canonical
    statuses = [None, None, "TOTALLY_WRONG"]  # 3rd row: soccer status outside the closed set
    checked, canonical, violations = m._assert_prediction_records_canonical(ids, types, statuses)
    assert checked == 3
    assert canonical == 0
    joined = " | ".join(violations)
    assert "lowercase-instrument_type" in joined
    assert "underlying-leakage-instrument_type" in joined
    assert "noncanonical-af_fixture_match_status" in joined


def test_assert_records_null_fixture_status_is_not_a_soccer_violation() -> None:
    m = _load()
    # A non-soccer prediction row legitimately has af_fixture_match_status None/"" — must pass.
    checked, canonical, violations = m._assert_prediction_records_canonical(
        ["POLYMARKET:PREDICTION_MARKET:0xa", "POLYMARKET:PREDICTION_MARKET:0xb"],
        ["PREDICTION_MARKET", "PREDICTION_MARKET"],
        [None, ""],
    )
    assert (checked, canonical) == (2, 2)
    assert violations == []


def test_assert_records_flags_whitespace_and_empty_id() -> None:
    m = _load()
    checked, canonical, violations = m._assert_prediction_records_canonical(
        ["POLYMARKET:PREDICTION_MARKET:0x a", ""], ["PREDICTION_MARKET", "PREDICTION_MARKET"], [None, None]
    )
    assert (checked, canonical) == (2, 0)
    assert "whitespace" in violations[0]
    assert "empty-id" in violations[1]


def test_assert_records_length_mismatch_raises() -> None:
    m = _load()
    with pytest.raises(ValueError, match="same length"):
        m._assert_prediction_records_canonical(["x"], ["PREDICTION_MARKET"], [])


# ---------------------------------------------------------------------------
# (A) _run_prediction_canonical_cell
# ---------------------------------------------------------------------------


def test_canonical_cell_passes_on_canonical_rows(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "expected_write_prefix", lambda cell, day: "prefix/")
    monkeypatch.setattr(
        m,
        "_read_instruments_parquet_rows",
        lambda bucket, prefix, smoke_date, venue: (
            ["POLYMARKET:PREDICTION_MARKET:0xa"],
            ["PREDICTION_MARKET"],
            ["MATCHED"],
        ),
    )
    result = m._run_prediction_canonical_cell(_FakeCell("PREDICTION", "POLYMARKET"), "2026-07-01", "p")
    assert result.status == "passed", result.reason
    assert result.leg == "canonical"
    assert "checked=1 canonical=1 raw=0" in result.reason


def test_canonical_cell_fails_on_noncanonical_rows(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "expected_write_prefix", lambda cell, day: "prefix/")
    monkeypatch.setattr(
        m,
        "_read_instruments_parquet_rows",
        lambda bucket, prefix, smoke_date, venue: (["POLYMARKET:PREDICTION_MARKET:0xa"], ["prediction"], [None]),
    )
    result = m._run_prediction_canonical_cell(_FakeCell("PREDICTION", "POLYMARKET"), "2026-07-01", "p")
    assert result.status == "failed"
    assert "checked=1 canonical=0 raw=1" in result.reason


def test_canonical_cell_fails_honestly_when_no_parquet(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "expected_write_prefix", lambda cell, day: "prefix/")
    monkeypatch.setattr(m, "_read_instruments_parquet_rows", lambda bucket, prefix, smoke_date, venue: None)
    result = m._run_prediction_canonical_cell(_FakeCell("PREDICTION", "KALSHI"), "2026-07-01", "p")
    assert result.status == "failed"
    assert "canonical_no_instruments_parquet_at" in result.reason


# ---------------------------------------------------------------------------
# (B) _run_prediction_grain_cells (CQG cluster + market_lifecycle)
# ---------------------------------------------------------------------------


def _skip_result(m, skip_signal_found: bool):
    return m.ShardCheckResult(shard_label="x", leg="skip", status="passed", skip_signal_found=skip_signal_found)


def _force_result(m):
    return m.ShardCheckResult(shard_label="x", leg="force", status="passed")


def test_grain_cells_force_pass_when_both_grains_present(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "_manifest_row_present_any_dt", lambda bucket, venue, day, dts: True)
    monkeypatch.setattr(m, "_noncanonical_cqg_values", lambda bucket, venue, day: [])
    monkeypatch.setattr(
        m, "_list_market_lifecycle_objects", lambda bucket, day, venue: ["obj/market_lifecycle.parquet"]
    )
    cells = m._run_prediction_grain_cells(
        _FakeCell("PREDICTION", "POLYMARKET"), "2026-07-01", "force", _force_result(m), "p"
    )
    labels = {c.shard_label: c for c in cells}
    assert set(labels) == {
        "PREDICTION/POLYMARKET/prediction_canonical_question_group/2026-07-01",
        "PREDICTION/POLYMARKET/market_lifecycle/2026-07-01",
    }
    assert all(c.status == "passed" for c in cells), {c.shard_label: c.reason for c in cells}


def test_grain_cells_fail_when_cqg_bundle_missing_or_noncanonical(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "_manifest_row_present_any_dt", lambda bucket, venue, day, dts: False)
    monkeypatch.setattr(m, "_noncanonical_cqg_values", lambda bucket, venue, day: ["btc_up_down_daily"])
    monkeypatch.setattr(m, "_list_market_lifecycle_objects", lambda bucket, day, venue: [])
    cells = m._run_prediction_grain_cells(
        _FakeCell("PREDICTION", "KALSHI"), "2026-07-01", "force", _force_result(m), "p"
    )
    by_label = {c.shard_label: c for c in cells}
    cqg = by_label["PREDICTION/KALSHI/prediction_canonical_question_group/2026-07-01"]
    lc = by_label["PREDICTION/KALSHI/market_lifecycle/2026-07-01"]
    assert cqg.status == "failed"
    assert "cqg_bundle_manifest_row_missing" in cqg.reason
    assert "noncanonical_cqg_values" in cqg.reason
    assert lc.status == "failed"
    assert "no_market_lifecycle_parquet" in lc.reason


def test_grain_cells_skip_requires_skip_signal(monkeypatch) -> None:
    m = _load()
    monkeypatch.setattr(m, "resolve_test_bucket", lambda ag, pid: "fake-bucket")
    monkeypatch.setattr(m, "_manifest_row_present_any_dt", lambda bucket, venue, day, dts: True)
    monkeypatch.setattr(m, "_noncanonical_cqg_values", lambda bucket, venue, day: [])
    monkeypatch.setattr(m, "_list_market_lifecycle_objects", lambda bucket, day, venue: ["o/market_lifecycle.parquet"])

    # skip leg WITHOUT the main leg's skip signal -> both grains fail on skip_signal_not_found
    cells = m._run_prediction_grain_cells(
        _FakeCell("PREDICTION", "POLYMARKET"), "2026-07-01", "skip", _skip_result(m, skip_signal_found=False), "p"
    )
    assert all(c.status == "failed" and "skip_signal_not_found" in c.reason for c in cells)

    # skip leg WITH the skip signal + both grains present -> genuine skip proof
    cells = m._run_prediction_grain_cells(
        _FakeCell("PREDICTION", "POLYMARKET"), "2026-07-01", "skip", _skip_result(m, skip_signal_found=True), "p"
    )
    assert all(c.status == "passed" for c in cells)
    assert all(c.skip_proof == "genuine" for c in cells)


# ---------------------------------------------------------------------------
# RULE 11 — cross-asset-group byte-unchanged pins
# ---------------------------------------------------------------------------

# Dedup'd (asset_group, venue) shard-target counts (measured 2026-07-20 against the live UAC
# registry — DEFI 89->98: +4 for METEORA-SOLANA/LIFINITY-SOLANA/PHOENIX-SOLANA/PYTH-SOLANA +
# +5 for CHAINLINK-{ETHEREUM,ARBITRUM,BASE,OPTIMISM,POLYGON}, all newly wired into
# factory._ADAPTERS (BLK-0c7b82fe resolved). The prediction adaptation must not change any
# other AG's enumerated target set.
# CEFI 25→26 (2026-07-21): unified-api-contracts@11adf279 registered OKX-FUTURES/
# OKX-SWAP cefi venues (+ deregistered legacy DERIBIT-COMBO) — an already-committed,
# clean UAC change (not from this session's diff); this test's frozen count needed to
# catch up. Pre-existing drift, verified via `git stash` to reproduce on a clean tree.
# DEFI 98→99 (2026-07-21): unified-api-contracts@6bdbc31d registered the AAVE-ETHEREUM
# oracle_prices leg (aave_oracle adapter key) — lst_rate_honest_coverage plan Phase 1;
# AaveOracleReferenceDataAdapter now exists (factory._ADAPTERS + orchestrator/defi.py
# _STATIC_DEFI_VENUES landed in the same commit as this test update).
_PER_AG_TARGET_COUNTS = {"CEFI": 26, "DEFI": 99, "TRADFI": 7, "SPORTS": 7, "PREDICTION": 2}


def test_rule11_per_ag_dedup_target_counts_byte_unchanged() -> None:
    m = _load()
    from smoke_matrix import enumerate_cells

    for ag, expected in _PER_AG_TARGET_COUNTS.items():
        targets = m._dedupe_shard_targets(enumerate_cells(asset_group_filter=ag))
        assert len(targets) == expected, f"{ag} dedup'd target count drifted: {len(targets)} != {expected}"


def test_rule11_canonical_not_in_default_legs_but_valid() -> None:
    m = _load()
    assert m._parse_legs("force,skip,live") == ["force", "skip", "live"]  # default is byte-unchanged
    assert "canonical" in m._VALID_LEGS
    assert m._parse_legs("force,skip,canonical") == ["force", "skip", "canonical"]


@pytest.mark.parametrize(
    ("asset_group", "venue"),
    [("CEFI", "BINANCE-FUTURES"), ("DEFI", "AAVE_V3-POLYGON"), ("TRADFI", "CME"), ("SPORTS", "BETFAIR")],
)
def test_rule11_canonical_cell_skipped_for_non_prediction(asset_group: str, venue: str) -> None:
    m = _load()
    result = m._run_prediction_canonical_cell(_FakeCell(asset_group, venue), "2026-07-01", "p")
    assert result.status == "skipped"
    assert result.leg == "canonical"
    assert result.reason == "canonical_shape_check_is_prediction_only"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
