#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: campaign
# Delete-when: SOURCE_RATE_LIMITS_RPM + SOURCE_PER_IP_LIMITS have measured (calibrated=True) values for understat / transfermarkt / open_meteo / soccer_football_info / polymarket and the operator confirms no further re-calibration is pending
"""One-time ramp-to-429 calibration probe (operator 2026-06-23: "blast from an IP,
see when we get banned").

For an UNDOCUMENTED-ceiling source — understat / transfermarkt / open_meteo /
soccer_football_info (Part 2) and the LIKELY-per-IP polymarket_clob /
polymarket_gamma_api (Part 3) — this fires a controlled, monotonically RISING
request rate from a SINGLE IP against a cheap read-only endpoint until the source
starts rejecting (HTTP 429 / 403 / connection-reset / ban), records the BREAK rate,
then probes the RECOVERY window (how long until requests succeed again).

**RUN FROM AN EPHEMERAL VM IP ONLY** — never a shared production egress IP. A
temporary ban on the probe VM's IP is acceptable (operator-sanctioned); a ban on a
production IP would stall the live backfill fleet. Launch a throwaway VM, run this,
read the JSON result, tear the VM down. The probe IS destructive to its own IP by
design (it intentionally trips the ban).

The output JSON carries, per source:
  * ``break_rate_rpm``     — the rpsx60 at which rejection first occurred
  * ``last_ok_rate_rpm``   — the highest rate that stayed fully 2xx
  * ``safe_rate_rpm``      — recommended limit = floor(0.8 x break_rate) (80% margin)
  * ``recovery_seconds``   — measured seconds from first-reject to next success
  * ``status_at_break``    — the HTTP status / error class observed at the break

The operator transcribes ``safe_rate_rpm`` + ``recovery_seconds`` into
``deployment-service .../launch_budget_registry.py`` (``SOURCE_RATE_LIMITS_RPM`` for
fleet-divided sources, ``SOURCE_PER_IP_LIMITS`` for per-IP) replacing the
``# TODO: empirically calibrate`` placeholders + flipping ``calibrated=True``, and
records the measured (break-rate, safe-rate, recovery) row in the plan.

Usage::

    # one source, default ramp:
    python scripts/calibrate_source_rate_limit.py --source understat

    # all undocumented sources, custom ramp + ceiling:
    python scripts/calibrate_source_rate_limit.py --all --start-rpm 30 --step-rpm 30 --max-rpm 1800

    # write the machine-readable result for transcription:
    python scripts/calibrate_source_rate_limit.py --all --out /tmp/calib.json

This is a SCRIPT (one-off campaign), not a CLI subcommand: it is throwaway probing,
not production runtime. It obeys repo SSOTs — UTC datetimes, aiohttp (async), no
hardcoded creds (api keys, where a source needs one, come from Secret Manager via
``get_secret_client``). Network access + a real (sacrificial) IP are REQUIRED; it is
inert in the credential-free / ``--block-network`` test sandbox by design.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import aiohttp

logger = logging.getLogger("calibrate_source_rate_limit")

# Cheap, read-only probe endpoints per source — a single lightweight GET that is
# safe to hammer (no writes, no auth-cost). These are the public/landing reads the
# adapters already hit; the probe does NOT need a full canonical fetch, only a
# request the source's rate-limiter counts. Endpoints that need a key read it from
# Secret Manager at runtime (resolved in _probe_once via _maybe_api_key).
PROBE_ENDPOINTS: dict[str, str] = {
    # Understat — public scrape; the league landing page is the cheapest counted GET.
    "understat": "https://understat.com/league/EPL",
    # Transfermarkt — public scrape; the competition landing page.
    "transfermarkt": "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
    # Open-Meteo — free forecast API; a tiny single-point forecast request.
    "open_meteo": "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.12&hourly=temperature_2m",
    # SoccerFootball-Info (RapidAPI) — health/landing probe (real plan would key it).
    "soccer_football_info": "https://soccer-football-info.p.rapidapi.com/",
    # Polymarket CLOB — public markets list (likely per-IP throttled).
    "polymarket_clob": "https://clob.polymarket.com/markets",
    # Polymarket Gamma — public markets API.
    "polymarket_gamma_api": "https://gamma-api.polymarket.com/markets?limit=1",
}

# Sources we model as PER-IP (Part 3) — the calibrated number lands in
# SOURCE_PER_IP_LIMITS (each VM/IP gets the full value), NOT the fleet-divided
# SOURCE_RATE_LIMITS_RPM. Recorded in the result so the operator transcribes to the
# right registry.
PER_IP_SOURCES: frozenset[str] = frozenset({"polymarket_clob", "polymarket_gamma_api", "databento"})

# A response is a "reject" (the source pushing back) at any of these statuses, or on
# a connection error / reset (a hard ban often manifests as a dropped connection).
_REJECT_STATUSES: frozenset[int] = frozenset({429, 403, 503, 509})
_SAFETY_MARGIN: float = 0.8  # recommended safe limit = 80% of the break rate.


@dataclass
class CalibrationResult:
    """The measured ceiling + recovery for one source (operator transcribes this)."""

    source: str
    per_ip: bool
    break_rate_rpm: int | None
    last_ok_rate_rpm: int
    safe_rate_rpm: int | None
    recovery_seconds: float | None
    status_at_break: str
    probed_at_utc: str
    note: str


def _maybe_api_key(source: str, project_id: str | None) -> str | None:
    """Resolve a Secret-Manager key for sources that need auth (never os.environ)."""
    secret_name = {
        "soccer_football_info": "rapidapi-key",
    }.get(source)
    if secret_name is None:
        return None
    from unified_trading_library import get_secret_client

    client = get_secret_client(project_id=project_id or None)
    return client.get_secret(secret_name) if client is not None else None


async def _fire_burst(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    n: int,
) -> tuple[int, str]:
    """Fire ``n`` concurrent GETs within ~1 second; return (#rejects, first-status).

    A burst of ``n`` over one second IS a rate of ``n`` rps ≈ ``nx60`` rpm against
    the source's per-second/per-minute window. Returns how many were rejected and a
    label for the first reject observed (status code or error class).
    """

    async def one() -> tuple[bool, str]:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status in _REJECT_STATUSES:
                    return True, f"HTTP {resp.status}"
                return False, f"HTTP {resp.status}"
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            # A dropped/reset connection is a likely hard-ban signal.
            return True, f"{type(exc).__name__}"

    results = await asyncio.gather(*(one() for _ in range(n)))
    rejects = sum(1 for r, _ in results if r)
    first_reject_label = next((label for r, label in results if r), "")
    return rejects, first_reject_label


async def _measure_recovery(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    max_wait_s: float,
) -> float | None:
    """After a break, poll once/second until a 2xx returns; return seconds waited."""
    start = datetime.now(UTC)
    waited = 0.0
    while waited < max_wait_s:
        await asyncio.sleep(1.0)
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status not in _REJECT_STATUSES and resp.status < 400:
                    return (datetime.now(UTC) - start).total_seconds()
        except (aiohttp.ClientError, TimeoutError, OSError):
            pass
        waited = (datetime.now(UTC) - start).total_seconds()
    return None  # did not recover within max_wait_s


async def calibrate_source(
    source: str,
    *,
    start_rpm: int,
    step_rpm: int,
    max_rpm: int,
    recovery_max_s: float,
    project_id: str | None,
) -> CalibrationResult:
    """Ramp the request rate against ``source`` until it rejects; measure recovery."""
    url = PROBE_ENDPOINTS.get(source)
    if url is None:
        raise ValueError(f"no probe endpoint for source {source!r}; known: {sorted(PROBE_ENDPOINTS)}")
    headers: dict[str, str] = {"User-Agent": "uts-rate-calibration-probe/1.0"}
    key = _maybe_api_key(source, project_id)
    if key is not None:
        headers["x-rapidapi-key"] = key

    last_ok_rpm = 0
    break_rpm: int | None = None
    status_at_break = ""
    recovery_seconds: float | None = None

    async with aiohttp.ClientSession() as session:
        rate_rpm = start_rpm
        while rate_rpm <= max_rpm:
            per_second = max(1, rate_rpm // 60)
            rejects, label = await _fire_burst(session, url, headers, per_second)
            logger.info(
                "[%s] rate=%d rpm (%d rps) → rejects=%d/%d %s", source, rate_rpm, per_second, rejects, per_second, label
            )
            if rejects > 0:
                break_rpm = rate_rpm
                status_at_break = label
                recovery_seconds = await _measure_recovery(session, url, headers, recovery_max_s)
                break
            last_ok_rpm = rate_rpm
            await asyncio.sleep(1.0)  # let the per-second window roll before the next step
            rate_rpm += step_rpm

    safe_rpm = int(break_rpm * _SAFETY_MARGIN) if break_rpm is not None else None
    note = (
        "BROKE at measured rate"
        if break_rpm is not None
        else f"no reject up to max_rpm={max_rpm}; ceiling is >= {last_ok_rpm} (raise --max-rpm to find it)"
    )
    return CalibrationResult(
        source=source,
        per_ip=source in PER_IP_SOURCES,
        break_rate_rpm=break_rpm,
        last_ok_rate_rpm=last_ok_rpm,
        safe_rate_rpm=safe_rpm,
        recovery_seconds=recovery_seconds,
        status_at_break=status_at_break,
        probed_at_utc=datetime.now(UTC).isoformat(),
        note=note,
    )


async def _run(args: argparse.Namespace) -> int:
    sources: list[str] = sorted(PROBE_ENDPOINTS) if args.all else [args.source] if args.source else []
    if not sources:
        logger.error("specify --source <name> or --all")
        return 2
    results: list[CalibrationResult] = []
    for src in sources:
        try:
            res = await calibrate_source(
                src,
                start_rpm=args.start_rpm,
                step_rpm=args.step_rpm,
                max_rpm=args.max_rpm,
                recovery_max_s=args.recovery_max_s,
                project_id=args.project_id,
            )
        except ValueError as exc:
            logger.error("skip %s: %s", src, exc)
            continue
        results.append(res)
        print(json.dumps(asdict(res), indent=2))

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump([asdict(r) for r in results], fh, indent=2)
        logger.info("wrote %d result(s) to %s", len(results), args.out)
    # Transcription reminder (the registry is the SSOT, not this probe output).
    print(
        "\nTranscribe each safe_rate_rpm + recovery_seconds into "
        "deployment-service/.../launch_budget_registry.py:\n"
        "  per_ip=False → SOURCE_RATE_LIMITS_RPM[source] = safe_rate_rpm (flip calibrated marker / drop TODO)\n"
        "  per_ip=True  → SOURCE_PER_IP_LIMITS[source] = PerIpLimit(rpm=safe_rate_rpm, calibrated=True, note=...)\n"
        "and record the measured (break, safe, recovery) row in the plan.",
        file=sys.stderr,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ramp-to-429 source rate-limit calibration probe.")
    parser.add_argument("--source", help="single source key (see PROBE_ENDPOINTS)")
    parser.add_argument("--all", action="store_true", help="probe every source in PROBE_ENDPOINTS")
    parser.add_argument("--start-rpm", type=int, default=60, help="initial probe rate (req/min)")
    parser.add_argument("--step-rpm", type=int, default=60, help="rate increment per step (req/min)")
    parser.add_argument("--max-rpm", type=int, default=3600, help="stop ramping at this ceiling (req/min)")
    parser.add_argument("--recovery-max-s", type=float, default=120.0, help="max seconds to wait for recovery")
    parser.add_argument("--project-id", default=None, help="GCP project for Secret-Manager-keyed sources")
    parser.add_argument("--out", default=None, help="write JSON results here for transcription")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
