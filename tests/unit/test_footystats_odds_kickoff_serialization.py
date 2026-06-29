"""Regression: footystats odds NaN-fill must emit string ``kickoff_utc``.

Guards the 2026-06-29 defect where the odds backfill crashed on every date that
fell through to the scheduled-fixture NaN-fill path:

    validation error in instruments-service.footystats_odds_fetch:
    ("Expected bytes, got a 'Timestamp' object",
     'Conversion failed for column kickoff_utc with type object')

The scheduled-fixture map holds ``pd.Timestamp`` kickoffs; the ``footystats_odds``
table types ``kickoff_utc`` as a string, so the NaN-fill rows must serialize the
Timestamp to an ISO string before the parquet write (``_kickoff_iso_or_none``).
"""

import pandas as pd
import pyarrow as pa
import pytest

from instruments_service.engine.orchestrator.footystats import _kickoff_iso_or_none


def test_kickoff_iso_or_none_serializes_timestamp() -> None:
    ko = pd.Timestamp("2025-09-03T15:00:00", tz="UTC")
    out = _kickoff_iso_or_none(ko)
    assert isinstance(out, str)
    # round-trips back to the same instant
    assert pd.to_datetime(out, utc=True) == ko


def test_kickoff_iso_or_none_passes_through_none() -> None:
    assert _kickoff_iso_or_none(None) is None


def test_nan_fill_rows_are_parquet_serializable() -> None:
    """A frame mixing an API string row + NaN-fill rows must serialize to arrow.

    This is the exact failure mode: a string ``kickoff_utc`` column that also
    contains a Timestamp object is rejected by pyarrow.
    """
    sched = {
        "EPL:1:2:2025-09-03": pd.Timestamp("2025-09-03T15:00:00", tz="UTC"),
        "EPL:3:4:2025-09-03": pd.Timestamp("2025-09-03T17:30:00", tz="UTC"),
    }
    rows = [{"canonical_fixture_id": "EPL:9:9:2025-09-03", "kickoff_utc": "2025-09-03T12:00:00+00:00"}]
    rows += [{"canonical_fixture_id": fid, "kickoff_utc": _kickoff_iso_or_none(ko)} for fid, ko in sched.items()]
    df = pd.DataFrame(rows)

    table = pa.Table.from_pandas(df)  # must not raise
    assert pa.types.is_string(table.schema.field("kickoff_utc").type)


def test_raw_timestamp_in_string_column_reproduces_the_bug() -> None:
    """Documents the pre-fix failure: a Timestamp in a string-typed column raises."""
    df = pd.DataFrame(
        [
            {"canonical_fixture_id": "EPL:9:9:2025-09-03", "kickoff_utc": "2025-09-03T12:00:00+00:00"},
            {"canonical_fixture_id": "EPL:1:2:2025-09-03", "kickoff_utc": pd.Timestamp("2025-09-03T15:00:00", tz="UTC")},
        ]
    )
    schema = pa.schema([("canonical_fixture_id", pa.string()), ("kickoff_utc", pa.string())])
    with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid)):
        pa.Table.from_pandas(df, schema=schema)
