"""Rolling-window CLI flag resolution for instruments-service.

Resolves ``--lookback-days`` / ``--lookahead-days`` / ``--force-window`` into
explicit ``--start-date`` / ``--end-date`` / ``--force`` flags before the
UTL ``ServiceBootstrap`` / ``ServiceCLI`` parses argv.

Why pre-parse rather than handler preflight: UTL's ``_Adapter`` in
``service_framework/_adapter.py`` builds ``BatchIO`` from ``args.start_date`` /
``args.end_date`` in ``_build_io()`` **before** ``handler.preflight()`` runs,
so preflight cannot retroactively change the batch date range. Pre-parsing
sys.argv is the only seam that keeps rolling-window resolution local to
instruments-service (no UTL hoist) while still propagating to BatchIO.

SSOT: ``unified-trading-pm/codex/02-data/sports-scheduling-and-sharding.md`` §4.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta


class RollingWindowError(ValueError):
    """Raised for invalid / conflicting rolling-window CLI flags."""


def resolve_rolling_window_args(
    argv: list[str],
    *,
    today: date | None = None,
) -> list[str]:
    """Return a new argv with rolling-window flags expanded.

    - ``--lookback-days N`` / ``--lookahead-days M`` resolve to
      ``--start-date (today - N)`` / ``--end-date (today + M)``.
    - ``--force-window`` adds ``--force`` (if absent) so the orchestrator's
      skip-if-exists freshness check is disabled for the computed window.

    Mutual-exclusion: if ``--start-date`` or ``--end-date`` is also supplied
    with any rolling flag, raises ``RollingWindowError``.

    Negative values for lookback/lookahead raise ``RollingWindowError``.

    ``today`` is injectable for tests; defaults to UTC wall-clock.
    """

    lookback = _extract_int_flag(argv, "--lookback-days")
    lookahead = _extract_int_flag(argv, "--lookahead-days")
    force_window = _has_flag(argv, "--force-window")
    has_explicit_start = _has_value_flag(argv, "--start-date")
    has_explicit_end = _has_value_flag(argv, "--end-date")

    has_rolling = lookback is not None or lookahead is not None

    if not has_rolling and not force_window:
        return list(argv)

    if has_rolling and (has_explicit_start or has_explicit_end):
        raise RollingWindowError(
            "Cannot combine --start-date/--end-date with "
            "--lookback-days/--lookahead-days. Pick one: explicit dates for "
            "historical backfill, rolling flags for forward-poll."
        )

    if lookback is not None and lookback < 0:
        raise RollingWindowError(f"--lookback-days must be >= 0 (got {lookback}).")
    if lookahead is not None and lookahead < 0:
        raise RollingWindowError(f"--lookahead-days must be >= 0 (got {lookahead}).")

    resolved = list(argv)

    if has_rolling:
        anchor = today if today is not None else datetime.now(UTC).date()
        start = anchor - timedelta(days=lookback or 0)
        end = anchor + timedelta(days=lookahead or 0)
        resolved.extend(["--start-date", start.isoformat(), "--end-date", end.isoformat()])

    if force_window and "--force" not in resolved:
        resolved.append("--force")

    return resolved


def _extract_int_flag(argv: list[str], name: str) -> int | None:
    """Return the int value of ``--flag N`` or ``--flag=N`` if present, else None.

    Raises RollingWindowError on malformed values.
    """
    for i, token in enumerate(argv):
        if token == name:
            if i + 1 >= len(argv):
                raise RollingWindowError(f"{name} requires a value.")
            try:
                return int(argv[i + 1])
            except ValueError as exc:
                raise RollingWindowError(f"{name} expects an integer, got {argv[i + 1]!r}.") from exc
        if token.startswith(f"{name}="):
            raw = token.split("=", 1)[1]
            try:
                return int(raw)
            except ValueError as exc:
                raise RollingWindowError(f"{name} expects an integer, got {raw!r}.") from exc
    return None


def _has_flag(argv: list[str], name: str) -> bool:
    """True if a bare ``--flag`` is anywhere in argv."""
    return name in argv


def _has_value_flag(argv: list[str], name: str) -> bool:
    """True if ``--flag X`` or ``--flag=X`` is present with a non-empty value."""
    for i, token in enumerate(argv):
        if token == name and i + 1 < len(argv) and argv[i + 1]:
            return True
        if token.startswith(f"{name}="):
            _, _, value = token.partition("=")
            if value:
                return True
    return False
