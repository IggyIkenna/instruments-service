#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
"""Phase 5 IS backfill: populate instruments-store-defi with archive-metadata fields.

Runs instruments-service in batch mode for Solana DeFi venues (Drift, Phoenix,
Marinade, Jito) to populate the instruments-store-defi parquets with the new
archive-metadata fields introduced in Phase 2 (IS@919c1e2):

  - ``source_archive_url_template``
  - ``source_record_types``
  - ``source_coverage_start``
  - ``source_coverage_end``

These fields allow MTDS handlers to derive S3/API URLs from InstrumentRecord
rather than hardcoding them (the architectural contract from Phase 3).

Command issued (one per venue chunk)::

    python -m instruments_service.cli.main \\
        --asset-group defi \\
        --mode batch \\
        --start-date <start> \\
        --end-date <end>

Drift archive window: 2021-11-05 (V2 launch) → 2025-01-08 (S3 archive end).
Phoenix/Marinade/Jito: use the same window (conservative).

Dry-run (default): prints the commands, no writes.
Apply (--apply --confirm): runs the IS CLI sequentially.

Usage::

    # Dry run:
    GCP_PROJECT_ID=central-element-323112 \\
    python3 scripts/backfill_solana_defi_is_phase5.py

    # Apply (VM with ADC + MANIFEST_PER_VM_SHARDS):
    GCP_PROJECT_ID=central-element-323112 \\
    VM_NAME=hk-slot8-is-phase5 \\
    MANIFEST_PER_VM_SHARDS=true \\
    python3 scripts/backfill_solana_defi_is_phase5.py --apply --confirm

execution:
  owner: operator (Tab 8 / slot-8 — Phase 5b of is_mtds_contract_audit_2026_05_20)
  cadence: one-shot
  verifier: instruments-store-defi DRIFT-SOLANA parquet for any date in window
            contains non-null source_archive_url_template column
  last_executed: NEVER
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import date

logger = logging.getLogger(__name__)

# Drift V2 mainnet launch (earliest meaningful date for S3 archive).
_SOLANA_DEFI_START = date(2021, 11, 5)
# Drift V1 S3 archive coverage end (matches _DRIFT_S3_ARCHIVE_END in MTDS handler).
_SOLANA_DEFI_END = date(2025, 1, 8)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 5 IS backfill — Solana DeFi archive-metadata fields",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute IS CLI run (default: dry-run, prints command only)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Suppress interactive confirmation prompt (required for --apply in CI/VM)",
    )
    parser.add_argument(
        "--start-date",
        default=_SOLANA_DEFI_START.isoformat(),
        help=f"Backfill start date (default: {_SOLANA_DEFI_START.isoformat()})",
    )
    parser.add_argument(
        "--end-date",
        default=_SOLANA_DEFI_END.isoformat(),
        help=f"Backfill end date (default: {_SOLANA_DEFI_END.isoformat()})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _validate_apply_env() -> bool:
    missing: list[str] = []
    if not os.environ.get("VM_NAME"):
        missing.append("VM_NAME=<unique-tag>")
    if os.environ.get("MANIFEST_PER_VM_SHARDS") != "true":
        missing.append("MANIFEST_PER_VM_SHARDS=true")
    if not os.environ.get("GCP_PROJECT_ID"):
        missing.append("GCP_PROJECT_ID=central-element-323112")
    if missing:
        logger.error("Apply mode requires:\n%s", "\n".join(f"  {m}" for m in missing))
        return False
    return True


def _build_is_cmd(start: str, end: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "instruments_service.cli.main",
        "--asset-group",
        "defi",
        "--mode",
        "batch",
        "--start-date",
        start,
        "--end-date",
        end,
    ]


def _run(*, start_date: str, end_date: str, apply: bool, confirm: bool) -> int:
    cmd = _build_is_cmd(start_date, end_date)

    logger.info("%s IS defi batch %s → %s", "Running" if apply else "DRY RUN —", start_date, end_date)
    logger.info("Command: %s", " ".join(cmd))
    logger.info(
        "What this populates: instruments-store-defi DRIFT-SOLANA + PHOENIX-SOLANA + "
        "MARINADE-SOLANA + JITO-SOLANA parquets with source_archive_url_template field."
    )

    if not apply:
        logger.info("DRY RUN: add --apply --confirm to execute.")
        return 0

    if not confirm:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        days = (end - start).days + 1
        print(
            f"\nAbout to run IS defi batch for {days} dates ({start_date} → {end_date}).\n"
            "This writes archive-metadata fields to instruments-store-defi.\n"
            "Add --confirm to skip this prompt.\nProceed? [y/N] ",
            end="",
        )
        answer = input()
        if answer.strip().lower() != "y":
            logger.info("Aborted by operator.")
            return 0

    env = os.environ.copy()
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        logger.error("IS defi batch FAILED (returncode=%d)", result.returncode)
        return 1

    logger.info("IS defi batch complete.")
    logger.info(
        "Verify: gsutil cat "
        "'gs://instruments-store-defi-central-element-323112/"
        "instrument_availability/by_date/day=2024-01-01/venue=DRIFT-SOLANA/instruments.parquet' "
        '| python3 -c "import io, pandas, sys; '
        "df=pandas.read_parquet(io.BytesIO(sys.stdin.buffer.read())); "
        "print(df[['source_archive_url_template']].dropna().head())\""
    )
    return 0


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.apply and not _validate_apply_env():
        sys.exit(1)

    sys.exit(
        _run(
            start_date=args.start_date,
            end_date=args.end_date,
            apply=args.apply,
            confirm=args.confirm,
        )
    )


if __name__ == "__main__":
    main()
