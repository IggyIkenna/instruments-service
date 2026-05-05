"""Unit tests for aggregate_processed_options_to_chain_bundle.parse_option_filename.

Validates the Databento canonical option-id parser used by the chain-bundle
migration script. Coverage: standard ES/MES roots, weekly EW1-4 roots,
AM-settled E1A-5A roots, EOM monthly, edge cases (futures filenames not parsed,
malformed names returning None).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "aggregate_processed_options_to_chain_bundle.py"
    spec = importlib.util.spec_from_file_location("aggregate_processed_options", script)
    if spec is None or spec.loader is None:
        msg = f"Failed to load script spec at {script}"
        raise RuntimeError(msg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        # ES quarterly options
        ("CME:OPTION:ESH4-C5000.parquet", ("ES", "H4", "C", 5000.0)),
        ("CME:OPTION:ESM4-P4500.parquet", ("ES", "M4", "P", 4500.0)),
        ("ESH4-C5000.parquet", ("ES", "H4", "C", 5000.0)),  # bare form
        ("ESM4-P5050.5.parquet", ("ES", "M4", "P", 5050.5)),  # fractional strike
        # EW = end-of-month options on ES
        ("CME:OPTION:EWM4-C5100.parquet", ("EW", "M4", "C", 5100.0)),
        # Weekly options EW1-EW4
        ("CME:OPTION:EW1M4-C5000.parquet", ("EW1", "M4", "C", 5000.0)),
        ("CME:OPTION:EW2N0-P3000.parquet", ("EW2", "N0", "P", 3000.0)),
        ("CME:OPTION:EW3Q5-C4900.parquet", ("EW3", "Q5", "C", 4900.0)),
        ("CME:OPTION:EW4Z6-P4800.parquet", ("EW4", "Z6", "P", 4800.0)),
        # AM-settled weekly E1A-E5A
        ("CME:OPTION:E1AN0-C3090.parquet", ("E1A", "N0", "C", 3090.0)),
        ("CME:OPTION:E2AG4-P5050.parquet", ("E2A", "G4", "P", 5050.0)),
        ("CME:OPTION:E5AZ6-C5500.parquet", ("E5A", "Z6", "C", 5500.0)),
        # EOM monthly
        ("CME:OPTION:EOMN0-C3000.parquet", ("EOM", "N0", "C", 3000.0)),
        ("CME:OPTION:EOMU5-P4750.parquet", ("EOM", "U5", "P", 4750.0)),
        # MES (micro)
        ("CME:OPTION:MESH4-C5000.parquet", ("MES", "H4", "C", 5000.0)),
    ],
)
def test_parse_option_filename_valid(mod, name: str, expected: tuple[str, str, str, float]) -> None:
    parsed = mod.parse_option_filename(name)
    assert parsed is not None
    assert parsed == expected


@pytest.mark.parametrize(
    "name",
    [
        # Futures (no -C/P{strike} suffix) — must NOT parse as option
        "CME:FUTURE:ESH4.parquet",
        "ESH4.parquet",
        # Bare ticks file (legacy)
        "ticks.parquet",
        "ticks_migrated_20260419T055627Z.parquet",
        # Equity / non-CME
        "NASDAQ:EQUITY:AAPL-USD.parquet",
        # Malformed
        "ESH4-X5000.parquet",  # bad put_call
        "ES-C5000.parquet",  # missing expiry
        "",
        "random_garbage.parquet",
    ],
)
def test_parse_option_filename_rejects(mod, name: str) -> None:
    assert mod.parse_option_filename(name) is None


def test_root_extraction_strips_trailing_two_chars(mod) -> None:
    """Verify the strip-last-2-chars convention for chain-root extraction.

    Chain-prefix = root + month-letter + year-digit. Strip 2 chars => root.
    """
    cases = {
        "ESH4": "ES",  # 2-char root + H4
        "EWM4": "EW",  # 2-char root + M4
        "EW1M4": "EW1",  # 3-char root + M4
        "E1AN0": "E1A",
        "EOMN0": "EOM",
        "MESH4": "MES",
    }
    for chain_prefix, expected_root in cases.items():
        # Reach into the regex to confirm group structure
        synthetic = f"CME:OPTION:{chain_prefix}-C5000.parquet"
        parsed = mod.parse_option_filename(synthetic)
        assert parsed is not None
        root, expiry, _pc, _strike = parsed
        assert root == expected_root, f"root for {chain_prefix} should be {expected_root}, got {root}"
        assert len(expiry) == 2, f"expiry should be 2 chars, got {expiry!r}"
