#!/usr/bin/env python3
"""Profile a single instruments-service DERIBIT backfill chunk.

Wraps the CLI entrypoint with tracemalloc + RSS sampling so we can find which
allocator is responsible for the observed 1.8 GB -> 17 GB chunk-RAM growth.

Drops these into <out_dir>:
  rss.log              — psutil RSS / VMS sampled every <sample_secs>
  snapshot_<n>.bin     — pickled tracemalloc snapshots, taken every <snapshot_secs>
  top_allocators.txt   — sorted "filename:line size_diff" produced from snapshot diffs
  summary.json         — start/end RSS, peak RSS, top allocators, OOM flag

Usage (foreground):
    .venv/bin/python scripts/profile_deribit_chunk.py \
        --start-date 2020-06-01 --end-date 2020-06-30 \
        --out-dir /tmp/deribit-profile-<ts>

Background-able: redirect stderr/stdout to a log file; the script writes its
own progress to <out_dir>/runner.log.
"""
from __future__ import annotations

import argparse
import json
import linecache
import logging
import os
import pickle
import signal
import sys
import threading
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path

import psutil

logger = logging.getLogger("deribit_profile")

# Globals for the snapshot scheduler thread.
_stop_event = threading.Event()
_out_dir: Path | None = None
_snapshot_interval = 300  # 5 min
_rss_interval = 30  # 30 s
_snapshot_counter = 0
_peak_rss = 0
_oom_flag = False


def _rss_sampler(pid: int, log_path: Path) -> None:
    """Append RSS / VMS to log_path every _rss_interval seconds."""
    global _peak_rss
    proc = psutil.Process(pid)
    with log_path.open("a") as f:
        f.write("ts_iso,rss_mb,vms_mb,num_threads\n")
        f.flush()
        while not _stop_event.wait(_rss_interval):
            try:
                mem = proc.memory_info()
                rss_mb = mem.rss / (1024 * 1024)
                vms_mb = mem.vms / (1024 * 1024)
                _peak_rss = max(_peak_rss, mem.rss)
                threads = proc.num_threads()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return
            ts = datetime.now(UTC).isoformat()
            f.write(f"{ts},{rss_mb:.1f},{vms_mb:.1f},{threads}\n")
            f.flush()


def _snapshot_scheduler() -> None:
    """Take a tracemalloc snapshot every _snapshot_interval seconds."""
    global _snapshot_counter
    assert _out_dir is not None
    while not _stop_event.wait(_snapshot_interval):
        snap = tracemalloc.take_snapshot()
        _snapshot_counter += 1
        path = _out_dir / f"snapshot_{_snapshot_counter:03d}.bin"
        with path.open("wb") as f:
            pickle.dump(snap, f)
        logger.info("snapshot %d written: %s (%d traces)", _snapshot_counter, path, len(snap.traces))


def _take_final_snapshot() -> Path:
    """Force one last snapshot regardless of timer state."""
    global _snapshot_counter
    assert _out_dir is not None
    _snapshot_counter += 1
    snap = tracemalloc.take_snapshot()
    path = _out_dir / f"snapshot_{_snapshot_counter:03d}_final.bin"
    with path.open("wb") as f:
        pickle.dump(snap, f)
    logger.info("final snapshot written: %s (%d traces)", path, len(snap.traces))
    return path


def _diff_first_vs_last() -> str:
    """Build a top-30 allocator-growth report from snapshot_001 vs final snapshot."""
    assert _out_dir is not None
    snaps = sorted(_out_dir.glob("snapshot_*.bin"))
    if len(snaps) < 2:
        return "ONLY_ONE_SNAPSHOT — cannot diff. RSS log shows growth shape; snapshot is point-in-time."

    with snaps[0].open("rb") as f:
        first = pickle.load(f)
    with snaps[-1].open("rb") as f:
        last = pickle.load(f)

    diffs = last.compare_to(first, "filename")
    lines = ["=== top 30 file-level growth (last - first) ==="]
    for stat in diffs[:30]:
        size_diff_mb = stat.size_diff / (1024 * 1024)
        count_diff = stat.count_diff
        lines.append(f"  {size_diff_mb:+8.2f} MB   {count_diff:+8d} alloc   {stat.traceback}")

    lines.append("")
    lines.append("=== top 30 line-level growth ===")
    diffs_line = last.compare_to(first, "lineno")
    for stat in diffs_line[:30]:
        size_diff_mb = stat.size_diff / (1024 * 1024)
        count_diff = stat.count_diff
        loc = stat.traceback[0] if stat.traceback else None
        if loc is not None:
            src = linecache.getline(loc.filename, loc.lineno).strip()
            lines.append(f"  {size_diff_mb:+8.2f} MB  {count_diff:+8d} alloc  {loc.filename}:{loc.lineno}")
            lines.append(f"           {src}")
        else:
            lines.append(f"  {size_diff_mb:+8.2f} MB  {count_diff:+8d} alloc  <no traceback>")
    return "\n".join(lines)


def _signal_oom(_signum: int, _frame: object) -> None:
    """Handler for SIGTERM (often the OOM killer's first action)."""
    global _oom_flag
    _oom_flag = True
    logger.warning("SIGTERM received — likely OOM. Forcing final snapshot.")
    _stop_event.set()
    _take_final_snapshot()
    sys.exit(143)


def main() -> int:
    global _out_dir, _snapshot_interval, _rss_interval

    parser = argparse.ArgumentParser(description="Profile DERIBIT instruments-service chunk")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD chunk start")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD chunk end")
    parser.add_argument("--asset-group", default="CEFI")
    parser.add_argument("--venue", default="DERIBIT")
    parser.add_argument("--out-dir", required=True, help="Where to drop profile artefacts")
    parser.add_argument("--snapshot-secs", type=int, default=300)
    parser.add_argument("--rss-secs", type=int, default=30)
    parser.add_argument("--tracemalloc-frames", type=int, default=15)
    args = parser.parse_args()

    _out_dir = Path(args.out_dir)
    _out_dir.mkdir(parents=True, exist_ok=True)
    _snapshot_interval = args.snapshot_secs
    _rss_interval = args.rss_secs

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(_out_dir / "runner.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Start tracemalloc BEFORE the service imports anything heavy so we capture
    # the real growth, not initial baseline. Frames=15 keeps memory overhead
    # bounded while still letting us trace into pydantic/aiohttp internals.
    tracemalloc.start(args.tracemalloc_frames)
    logger.info("tracemalloc started with %d frames", args.tracemalloc_frames)

    # Capture baseline immediately (before service imports).
    pre_import_snap = tracemalloc.take_snapshot()
    with (_out_dir / "snapshot_000_preimport.bin").open("wb") as f:
        pickle.dump(pre_import_snap, f)

    # Spin up samplers.
    pid = os.getpid()
    rss_thread = threading.Thread(target=_rss_sampler, args=(pid, _out_dir / "rss.log"), daemon=True)
    snap_thread = threading.Thread(target=_snapshot_scheduler, daemon=True)
    rss_thread.start()
    snap_thread.start()

    signal.signal(signal.SIGTERM, _signal_oom)

    # Forge the argv ServiceBootstrap will parse. Mirror what
    # run_vm_backfill_e2e.sh passes per-chunk.
    sys.argv = [
        "instruments-service",
        "--operation", "instruments",
        "--mode", "batch",
        "--asset-group", args.asset_group.upper(),
        "--venues", args.venue.upper(),
        "--start-date", args.start_date,
        "--end-date", args.end_date,
    ]

    # Per-VM shard injection — same pattern as run_vm_backfill_e2e.sh.
    # Distinct VM tag so we don't race the canonical and the consolidator
    # can merge our writes cleanly.
    profile_tag = f"profile_{args.venue.lower()}_{args.start_date.replace('-', '')}"
    os.environ.setdefault("VM_NAME", profile_tag)
    os.environ.setdefault("MANIFEST_PER_VM_SHARDS", "true")
    os.environ.setdefault("GCP_PROJECT_ID", "central-element-323112")
    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("CLOUD_MOCK_MODE", "false")

    start_iso = datetime.now(UTC).isoformat()
    start_rss = psutil.Process().memory_info().rss
    logger.info("=== chunk start: venue=%s range=%s..%s rss=%.1f MB ===",
                args.venue, args.start_date, args.end_date, start_rss / 1024 / 1024)

    rc = 0
    try:
        from instruments_service.cli.main import main_service_cli
        main_service_cli()
    except SystemExit as e:
        rc = int(e.code) if isinstance(e.code, int) else 1
        logger.info("service exited with code %d", rc)
    except Exception:
        logger.exception("service raised — capturing final snapshot")
        rc = 99
    finally:
        _stop_event.set()
        end_rss = psutil.Process().memory_info().rss
        end_iso = datetime.now(UTC).isoformat()
        logger.info("=== chunk end: rss=%.1f MB peak=%.1f MB ===",
                    end_rss / 1024 / 1024, _peak_rss / 1024 / 1024)
        _take_final_snapshot()

        # Build the comparison report.
        report = _diff_first_vs_last()
        (_out_dir / "top_allocators.txt").write_text(report)

        summary = {
            "venue": args.venue,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "started_at": start_iso,
            "ended_at": end_iso,
            "rc": rc,
            "rss_start_mb": start_rss / 1024 / 1024,
            "rss_end_mb": end_rss / 1024 / 1024,
            "rss_peak_mb": _peak_rss / 1024 / 1024,
            "rss_growth_mb": (end_rss - start_rss) / 1024 / 1024,
            "snapshots_taken": _snapshot_counter,
            "oom_signal_received": _oom_flag,
        }
        (_out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        logger.info("summary: %s", summary)

    return rc


if __name__ == "__main__":
    sys.exit(main())
