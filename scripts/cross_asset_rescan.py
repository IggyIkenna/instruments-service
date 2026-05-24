#!/usr/bin/env python3
"""Cross-asset manifest rescan — Phase 3.A of manifest_schema_final_gate_2026_05_09.

Walks every asset_group's availability manifest + cross-checks each captured row
vs on-disk parquet existence across the 5 phantom-audit drift axes. Classifies
each disagreement into A/B/C per `manifest_cross_asset_rescan_design_2026_05_08`:

    A  Mutable  — capture_status / error_reason / attempted_at / path. Auto-flips
                  when VM_APPLY_FLIPS=true; dry-run otherwise.
    B  Immutable — pipeline_mode / available_at / service_emission_state. Never
                  flipped; logged if mismatched (would indicate writer bug).
    C  Triage   — asset_group / venue / data_type / instrument_type /
                  instrument_id / chain row-key drift. Streamed to
                  gs://{pid}-rescan-triage/{run_id}/triage.jsonl for operator.

Architecture: thin orchestrator on top of `reconcile_phantom_manifest_rows_all.py`
which already handles drift-axis probing for cefi/defi/sports. Adds:

    - asset_group=cross_asset_all dispatch over all 5 groups.
    - Per-VM shard isolation (MANIFEST_PER_VM_SHARDS=true + VM_NAME=...).
    - VM_APPLY_FLIPS=true gate (default false = dry-run + triage-only).
    - Class C triage JSONL stream to GCS for operator review.
    - Lifecycle events (STARTED / SHARD_PROCESSED / STOPPED / FAILED) to the
      events bucket so the deployment-UI deploy-missing flow surfaces progress.

Invocation (launcher's metadata sets these envvars):

    python -m instruments_service \\
        --operation cross_asset_rescan \\
        --mode batch \\
        --asset-group <cefi|defi|tradfi|sports|prediction|cross_asset_all> \\
        [--apply]

The launcher script `deployment-service/scripts/vm/launch-cross-asset-rescan-vm.sh`
sets WORKERS=64, HTTP_POOL_SIZE=128, MANIFEST_PER_VM_SHARDS=true,
VM_NAME=cross-asset-rescan-{RUN_TS}, VM_APPLY_FLIPS=<true|false>. See the
launcher for full env-var contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from google.cloud import storage
from unified_trading_library import GcsEventSink, log_event, setup_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
TRIAGE_BUCKET = f"{PROJECT_ID}-rescan-triage"
EVENTS_SERVICE = "instruments-service"

ASSET_GROUPS_ALL: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Pass → --data-types mapping for ordered reconciliation
# (manifest_cross_asset_rescan_design_2026_05_08.md § "Reconciliation dependency ordering").
# RESCAN_PASS="" (unset) or "all" = no data-type filter (backwards-compatible dry-run).
PASS_DATA_TYPES: dict[str, str] = {
    "1": "instruments,venue_trading_calendar",
    "2": (
        "ohlcv_1h,ohlcv_1m,ohlcv_24h,ohlcv_15m,trades,tbbo,book_snapshot_5,book_snapshot,"
        "lending_rates,lst_yields,lending_indices,dex_pools,dex_pool_swaps,perp_funding,"
        "oracle_prices,staking_yields,risk_params,rewards,flash_loan_events,governance_events,"
        "liquidation_events,bridge_events,position_data,token_transfers,vault_share_price,"
        "options_chain,futures_chain,prediction_canonical_question_group,MARKET_LIFECYCLE"
    ),
    # Pass 3 = MDPS pipeline_mode outputs. ohlcv_1h rows with pipeline_mode=pipeline
    # are distinguished from MTDS rows at the reconciler level via the on-disk path probe.
    "3": "ohlcv_1h",
    # Pass 4 = features families. Data-types TBD (vary per family + asset_group);
    # empty string = all remaining rows not already flipped by passes 1-3 (idempotent).
    "4": "",
}


def _env_flag(name: str, *, default: bool = False) -> bool:
    """Read a boolean env var (true/1/yes vs everything else)."""
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"true", "1", "yes", "on"}


def _vm_name() -> str:
    """Resolve the per-VM tag for shard isolation.

    Launcher sets VM_NAME=cross-asset-rescan-{RUN_TS}. If unset (local dev),
    synthesize one with the same prefix so concurrency guards still fire.
    """
    name = os.environ.get("VM_NAME", "").strip()
    if name:
        return name
    return f"cross-asset-rescan-local-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"


def _run_id() -> str:
    """Derive the operator-facing run identifier from VM_NAME.

    Strips the `cross-asset-rescan-` prefix so triage paths read as the bare
    timestamp the operator launched. Falls back to VM_NAME verbatim if the
    prefix isn't present (shouldn't happen, but defensive).
    """
    name = _vm_name()
    prefix = "cross-asset-rescan-"
    return name.removeprefix(prefix) if name.startswith(prefix) else name


def _reconcile_script() -> Path:
    """Locate the existing phantom-audit script in the instruments-service repo."""
    here = Path(__file__).resolve()
    candidate = here.parent / "reconcile_phantom_manifest_rows_all.py"
    if not candidate.exists():
        raise FileNotFoundError(f"reconcile_phantom_manifest_rows_all.py not at {candidate}")
    return candidate


def _pass_data_types() -> str:
    """Return the --data-types filter string for the current RESCAN_PASS env var.

    RESCAN_PASS is set by the launcher when running a specific ordered pass (1-4).
    Unset or "all" means no filter (backwards-compatible: reconciler scans everything).
    """
    rescan_pass = os.environ.get("RESCAN_PASS", "").strip()
    if not rescan_pass or rescan_pass == "all":
        return ""
    return PASS_DATA_TYPES.get(rescan_pass, "")


def _run_per_asset_group_phantom_audit(
    *,
    asset_group: str,
    apply_flips: bool,
    triage_jsonl: Path,
) -> dict[str, int | str]:
    """Invoke the existing per-asset-group reconciler.

    The reconciler handles the 5 drift axes (hive-vocab, instrument_type casing,
    schema-4 empty instrument_type, path-prefix drift, chain-bundle equivalence)
    + writes class-A flips back to the canonical manifest when --apply is passed.
    We capture stdout for triage stream + return summary counts.

    For cross-asset-rescan semantics:
      apply_flips=True  → invokes reconciler WITHOUT --dry-run (writes flips).
      apply_flips=False → invokes with --dry-run (counts disagreements only).
    """
    reconcile = _reconcile_script()
    cmd = [
        sys.executable,
        str(reconcile),
        "--asset-group",
        asset_group,
    ]
    if not apply_flips:
        cmd.append("--dry-run")
    data_types = _pass_data_types()
    if data_types:
        cmd.extend(["--data-types", data_types])

    rescan_pass = os.environ.get("RESCAN_PASS", "all") or "all"
    log_event(
        "RESCAN_SHARD_STARTED",
        details={
            "asset_group": asset_group,
            "apply_flips": apply_flips,
            "vm_name": _vm_name(),
            "rescan_pass": rescan_pass,
            "data_types_filter": _pass_data_types() or "(all)",
        },
    )

    started = time.time()
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = time.time() - started

    if completed.returncode != 0:
        logger.error(
            "phantom-audit asset_group=%s failed rc=%d stderr=%s",
            asset_group,
            completed.returncode,
            completed.stderr[-2000:],
        )
        log_event(
            "RESCAN_SHARD_FAILED",
            details={
                "asset_group": asset_group,
                "return_code": completed.returncode,
                "elapsed_s": int(elapsed),
                "stderr_tail": completed.stderr[-500:],
            },
        )
        return {
            "asset_group": asset_group,
            "status": "failed",
            "return_code": completed.returncode,
            "elapsed_s": int(elapsed),
        }

    # Reconciler stdout includes phantom-row dumps + summary lines. Stream
    # any "PHANTOM" lines into triage.jsonl so class-C ambiguities reach the
    # operator. Phantom-row drift that the reconciler couldn't auto-classify
    # is the canonical class-C case per the design spec.
    phantom_lines = [line for line in completed.stdout.splitlines() if "PHANTOM" in line.upper()]
    with triage_jsonl.open("a", encoding="utf-8") as fh:
        for line in phantom_lines:
            fh.write(
                json.dumps(
                    {
                        "asset_group": asset_group,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "disagreement_class": "C",
                        "source": "reconcile_phantom_manifest_rows_all",
                        "raw": line,
                    }
                )
                + "\n"
            )

    log_event(
        "RESCAN_SHARD_COMPLETED",
        details={
            "asset_group": asset_group,
            "apply_flips": apply_flips,
            "elapsed_s": int(elapsed),
            "phantom_line_count": len(phantom_lines),
        },
    )
    return {
        "asset_group": asset_group,
        "status": "completed",
        "return_code": 0,
        "elapsed_s": int(elapsed),
        "phantom_line_count": len(phantom_lines),
    }


def _upload_triage(triage_local: Path, run_id: str) -> str:
    """Upload the run's triage.jsonl to gs://{pid}-rescan-triage/{run_id}/triage.jsonl.

    Returns the destination GCS URI. If the local file is empty (no class-C
    rows surfaced), uploads an empty marker file so operators can still spot
    that the run completed successfully with zero triage cases.
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(TRIAGE_BUCKET)
    blob_name = f"{run_id}/triage.jsonl"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(triage_local), content_type="application/jsonl")
    return f"gs://{TRIAGE_BUCKET}/{blob_name}"


def _ensure_triage_bucket_exists() -> None:
    """Best-effort: log a clear error if the triage bucket isn't provisioned.

    The bucket should be created by deployment-service infra; this function
    surfaces missing-bucket failures with a helpful provisioning hint rather
    than letting the upload step fail late.
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(TRIAGE_BUCKET)
    if not bucket.exists():
        logger.error(
            "triage bucket gs://%s does not exist. Provision via deployment-service "
            "infra before running the rescan VM.",
            TRIAGE_BUCKET,
        )
        log_event(
            "RESCAN_TRIAGE_BUCKET_MISSING",
            details={"bucket": TRIAGE_BUCKET, "project": PROJECT_ID},
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cross-asset manifest rescan: class-A auto-flip + class-C triage routing.",
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=("cefi", "defi", "tradfi", "sports", "prediction", "cross_asset_all"),
        help="Which asset_group to rescan. cross_asset_all walks every group.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=_env_flag("VM_APPLY_FLIPS", default=False),
        help="Apply class-A auto-flips. Default = dry-run (VM_APPLY_FLIPS env var).",
    )
    parser.add_argument(
        "--mode",
        default="batch",
        choices=("batch",),
        help="Operational mode (batch only for rescan; live makes no sense here).",
    )
    args = parser.parse_args()

    # setup_events() requires explicit mode + GcsEventSink in batch mode (UTL
    # contract; raises RuntimeError otherwise). Fix shipped 2026-05-11 after
    # `cross-asset-rescan-20260511-171623` failed with
    # `TypeError: setup_events() missing 1 required positional argument: 'mode'`.
    sink = GcsEventSink(
        project_id=PROJECT_ID,
        bucket=f"{PROJECT_ID}-events",
        service_name=EVENTS_SERVICE,
    )
    setup_events(service_name=EVENTS_SERVICE, mode=args.mode, sink=sink)

    vm_name = _vm_name()
    run_id = _run_id()
    log_event(
        "RESCAN_RUN_STARTED",
        details={
            "vm_name": vm_name,
            "run_id": run_id,
            "asset_group_arg": args.asset_group,
            "apply_flips": args.apply,
            "workers": int(os.environ.get("WORKERS", "1")),
            "http_pool_size": int(os.environ.get("HTTP_POOL_SIZE", "10")),
        },
    )

    _ensure_triage_bucket_exists()

    targets = ASSET_GROUPS_ALL if args.asset_group == "cross_asset_all" else (args.asset_group,)

    tmp_root = Path(tempfile.gettempdir())
    triage_local = tmp_root / f"rescan-triage-{run_id}.jsonl"
    triage_local.touch(exist_ok=True)

    results: list[dict[str, int | str]] = []
    for group in targets:
        results.append(
            _run_per_asset_group_phantom_audit(
                asset_group=group,
                apply_flips=args.apply,
                triage_jsonl=triage_local,
            )
        )

    triage_uri = _upload_triage(triage_local, run_id)
    logger.info("triage uploaded to %s", triage_uri)

    any_failed = any(r["status"] == "failed" for r in results)
    log_event(
        "RESCAN_RUN_STOPPED" if not any_failed else "RESCAN_RUN_FAILED",
        details={
            "vm_name": vm_name,
            "run_id": run_id,
            "triage_uri": triage_uri,
            "per_group_results": results,
            "apply_flips": args.apply,
        },
    )

    return 0 if not any_failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
