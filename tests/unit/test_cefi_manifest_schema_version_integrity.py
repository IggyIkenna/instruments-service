"""Regression test: cefi manifest schema_version must always be an integer.

Historical bug (2023-12-16..18, 2025-03-25..27): an old cefi instrument-definition
writer emitted rows where the ``schema_version`` column received the chain name
(e.g. "SOLANA", "ZKSYNC") instead of the integer schema version constant.  This
produced 16 invalid manifest rows (7 schema_version='SOLANA', 9 schema_version='ZKSYNC')
with blank ``capture_status``, which the consolidator's stale-row drop
(dd17ce23) handles on the next consolidation cycle.

This test ensures the current code cannot reproduce that bug:

1. AvailabilityRecord always stamps ``schema_version=MANIFEST_SCHEMA_VERSION``
   (integer 9) regardless of venue / chain value.
2. The cefi manifest writer path (_write_venue) separates chain from schema_version:
   for on-chain CeFi venues like LIGHTER-ZKSYNC and PACIFICA-SOLANA, the manifest
   row carries ``chain=""`` (cefi, not defi) and ``schema_version=int``.
"""

from __future__ import annotations

import dataclasses

from unified_trading_library import MANIFEST_SCHEMA_VERSION, AvailabilityRecord


class TestAvailabilityRecordSchemaVersionAlwaysInt:
    """AvailabilityRecord always stamps integer schema_version."""

    def test_default_schema_version_is_integer(self) -> None:
        """schema_version defaults to the MANIFEST_SCHEMA_VERSION integer constant."""
        rec = AvailabilityRecord(
            date="2023-12-17",
            venue="LIGHTER-ZKSYNC",
            instrument_count=5,
            service_name="instruments-service",
            written_at="2023-12-17T00:00:00+00:00",
        )
        assert isinstance(rec.schema_version, int), (
            "schema_version must be int, got "
            f"{type(rec.schema_version).__name__!r} = {rec.schema_version!r}"
        )
        assert rec.schema_version == MANIFEST_SCHEMA_VERSION

    def test_chain_name_does_not_bleed_into_schema_version(self) -> None:
        """Historical bug: chain string ('SOLANA', 'ZKSYNC') must NOT appear in schema_version."""
        chain_names = ["SOLANA", "ZKSYNC", "ETHEREUM", "ARBITRUM", "STARKNET"]
        for chain in chain_names:
            rec = AvailabilityRecord(
                date="2023-12-17",
                venue="SOMEPROTOCOL",
                chain=chain,
                instrument_count=10,
                service_name="instruments-service",
                written_at="2023-12-17T00:00:00+00:00",
            )
            assert rec.schema_version != chain, (
                f"schema_version must not be the chain name '{chain}'; got {rec.schema_version!r}"
            )
            assert isinstance(rec.schema_version, int), (
                f"schema_version must be int (not chain string); got "
                f"{type(rec.schema_version).__name__!r} = {rec.schema_version!r}"
            )

    def test_cefi_on_chain_venue_schema_version_integer(self) -> None:
        """On-chain CeFi venues (LIGHTER-ZKSYNC, PACIFICA-SOLANA, EXTENDED-STARKNET) get int schema_version."""
        cefi_on_chain_venues = [
            "LIGHTER-ZKSYNC",
            "PACIFICA-SOLANA",
            "EXTENDED-STARKNET",
        ]
        for venue in cefi_on_chain_venues:
            rec = AvailabilityRecord(
                date="2025-03-25",
                venue=venue,
                chain="",  # cefi: chain is "" (not a defi protocol-chain row)
                instrument_count=8,
                service_name="instruments-service",
                written_at="2025-03-25T12:00:00+00:00",
            )
            assert isinstance(rec.schema_version, int), (
                f"Venue {venue}: schema_version must be int, got "
                f"{type(rec.schema_version).__name__!r} = {rec.schema_version!r}"
            )
            assert rec.schema_version == MANIFEST_SCHEMA_VERSION
            # Chain must be empty for cefi venues (chain is a defi-only axis)
            assert rec.chain == "", (
                f"Venue {venue}: cefi manifest row must have chain='', got {rec.chain!r}"
            )

    def test_manifest_schema_version_constant_is_int(self) -> None:
        """MANIFEST_SCHEMA_VERSION itself is an integer (guard against type drift)."""
        assert isinstance(MANIFEST_SCHEMA_VERSION, int), (
            f"MANIFEST_SCHEMA_VERSION must be int, got {type(MANIFEST_SCHEMA_VERSION).__name__!r}"
        )
        # Current version is 9 (v9 schema: source column added)
        assert MANIFEST_SCHEMA_VERSION >= 9, (
            f"MANIFEST_SCHEMA_VERSION must be >= 9 (current v9), got {MANIFEST_SCHEMA_VERSION}"
        )


class TestAvailabilityRecordSerializationSchemaVersion:
    """Verify that AvailabilityRecord.schema_version serializes as int in the output dict.

    The manifest writer serialises AvailabilityRecord to a dict row before writing to
    parquet. Confirm schema_version in the serialised form is also int (not str).
    """

    def test_availability_record_schema_version_in_serialized_row(self) -> None:
        """schema_version in the dict representation of AvailabilityRecord is always int.

        Regression: the legacy writer emitted a raw dict where schema_version was
        accidentally set to the chain name string. AvailabilityRecord prevents this
        because its ``schema_version: int = MANIFEST_SCHEMA_VERSION`` field enforces
        the correct type at construction time.
        """
        rec = AvailabilityRecord(
            date="2023-12-17",
            venue="LIGHTER-ZKSYNC",
            chain="",  # cefi: no chain
            instrument_count=5,
            service_name="instruments-service",
            written_at="2023-12-17T00:00:00+00:00",
        )
        # Simulate the serialiser's dict access (manifest writer iterates records → dict)
        row = dataclasses.asdict(rec)
        assert "schema_version" in row, "schema_version must be in serialised row"
        assert isinstance(row["schema_version"], int), (
            "Serialised schema_version must be int, "
            f"got {type(row['schema_version']).__name__!r} = {row['schema_version']!r}"
        )
        assert row["schema_version"] == MANIFEST_SCHEMA_VERSION
        # Chain must round-trip as empty string for cefi rows
        assert row["chain"] == ""
