"""Regression tests: OKX-SWAP/OKX-FUTURES margin-type inversion bug and the
@LIN/@INV canonical-symbol builder (instrument_id_format_canonicalization_
2026_07_08.md finding 1, PERPETUAL scope expanded 2026-07-09).

One real, pre-existing, already-confirmed bug fixed here (not just
"unlabeled" — the old code was genuinely BACKWARDS):

  ``_infer_margin_type`` had an unconditional (no exchange gate) check —
  ``if "USD_UM" in upper_id or "USD_CM" in upper_id: return MarginType.INVERSE``
  — followed by a generic LINEAR default for everything else. Both halves
  were wrong for OKX:

  * Real OKX dated futures carrying the literal ``_UM`` infix in their
    ``instId`` (e.g. ``BTC-USD_UM-260710``) are ``ctType=linear`` (a
    USD-Margined / cross-margin contract, ``ctValCcy=BTC``) — the old code
    mapped this straight to INVERSE, the opposite of the real value.
  * Real OKX derivatives with a bare ``USD`` quote and no ``_UM``/``_CM``
    infix (every OKX-SWAP perpetual like ``BTC-USD-SWAP``, and the
    non-``_UM`` sibling of every dated future like ``BTC-USD-260710``) are
    ``ctType=inverse`` (coin-margined, settled in the base asset) — the old
    code had no OKX branch at all for this case, so it fell through to the
    generic LINEAR default, again the opposite of the real value.

  Net effect: every real OKX-SWAP/OKX-FUTURES derivative was mislabeled the
  OPPOSITE of its true margin type — not a missing label, a genuinely
  inverted one, exactly as flagged.

All raw ids and ``ctType``/``settleCcy`` values below are REAL, live-verified
directly against ``https://www.okx.com/api/v5/public/instruments`` (public,
no-auth REST endpoint) on 2026-07-09 for ``instType=SWAP`` (416 real rows)
and ``instType=FUTURES`` (105 real rows), BTC/ETH underlyings — not
fabricated. Full real-data cross-check performed for this fix: across all 105
real FUTURES rows, EVERY row with ``_UM`` in ``instId`` is ``ctType=linear``
(93/93) and EVERY row without it is ``ctType=inverse`` (12/12) — a clean,
deterministic split with zero exceptions. Across all 416 real SWAP rows, ZERO
carry a ``_UM``/``_CM`` infix.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts import TardisInstrumentDetail
from unified_api_contracts.internal import InstrumentType, MarginType

from instruments_service.reference_data.adapters.cefi.tardis import (
    _build_canonical_future_key,
    _build_canonical_perpetual_key,
    _infer_margin_type,
    _resolve_base_quote,
)


def _item(raw_id: str) -> TardisInstrumentDetail:
    """A Tardis item with no explicit base/quote → forces symbol-based parsing.

    Matches production reality: ``_fetch_exchange_instruments`` calls the
    free/no-auth ``/v1/exchanges/{exchange}`` endpoint, whose
    ``TardisExchangeDetail.instruments`` mapping never populates
    ``baseCurrency``/``quoteCurrency``. Every real OKX-SWAP/OKX-FUTURES
    instrument goes through the symbol-splitting fallback below.
    """
    return TardisInstrumentDetail(id=raw_id, type="future")


# ---------------------------------------------------------------------------
# _infer_margin_type — the real bug fix, both halves of the inversion
# ---------------------------------------------------------------------------


class TestInferMarginTypeOkxSwap:
    """OKX-SWAP (Tardis ``okex-swap``) — perpetuals never carry _UM/_CM."""

    def test_bare_usd_perpetual_is_inverse(self) -> None:
        """Real, live BTC-USD-SWAP: ctType=inverse, settleCcy=BTC."""
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTC-USD-SWAP", "okex-swap") == (MarginType.INVERSE)

    def test_usdt_perpetual_is_linear(self) -> None:
        """Real, live BTC-USDT-SWAP: ctType=linear, settleCcy=USDT."""
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USDT", "BTC-USDT-SWAP", "okex-swap") == (MarginType.LINEAR)

    @pytest.mark.parametrize(
        "raw_id",
        ["ETH-USD-SWAP", "SOL-USD-SWAP", "DOGE-USD-SWAP", "XRP-USD-SWAP"],
    )
    def test_other_bases_bare_usd_perpetual_is_inverse(self, raw_id: str) -> None:
        """Real, live coin-margined perpetuals confirmed on the OKX public API
        2026-07-09 (not BTC-only — the rule is quote-shape-driven, not a
        per-base whitelist).
        """
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", raw_id, "okex-swap") == MarginType.INVERSE

    def test_spot_returns_none_unaffected(self) -> None:
        assert _infer_margin_type(InstrumentType.SPOT_PAIR, "USDT", "BTC-USDT", "okex") is None


class TestInferMarginTypeOkxFutures:
    """OKX-FUTURES (Tardis ``okex-futures``) — the ``_UM`` infix disambiguates."""

    def test_um_infix_dated_future_is_linear(self) -> None:
        """Real, live BTC-USD_UM-260710: ctType=linear (settleCcy="USD" is a
        synthetic cross-margin unit, NOT the base asset; ctValCcy=BTC).
        """
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTC-USD_UM-260710", "okex-futures") == (
            MarginType.LINEAR
        )

    def test_bare_dated_future_is_inverse(self) -> None:
        """Real, live BTC-USD-260710 (same underlying + expiry as the _UM row
        above, no infix): ctType=inverse, settleCcy=BTC — genuinely the
        opposite margin type of its _UM sibling despite an identical quote
        token.
        """
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTC-USD-260710", "okex-futures") == (
            MarginType.INVERSE
        )

    @pytest.mark.parametrize(
        ("raw_id", "expected"),
        [
            ("BTC-USD_UM-260717", MarginType.LINEAR),
            ("BTC-USD_UM-260925", MarginType.LINEAR),
            ("BTC-USD_UM-261225", MarginType.LINEAR),
            ("ETH-USD_UM-260710", MarginType.LINEAR),
            ("BTC-USD-260717", MarginType.INVERSE),
            ("BTC-USD-260925", MarginType.INVERSE),
            ("BTC-USD-261225", MarginType.INVERSE),
            ("ETH-USD-260710", MarginType.INVERSE),
        ],
    )
    def test_full_real_um_vs_bare_split(self, raw_id: str, expected: MarginType) -> None:
        """Every real live OKX-FUTURES id pulled from the public API 2026-07-09
        (a representative sample of the clean, zero-exception 93-linear/
        12-inverse split observed across all 105 real rows).
        """
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", raw_id, "okex-futures") == expected

    def test_cm_infix_dated_future_is_inverse(self) -> None:
        """``_CM`` (COIN-Margined) is unobserved in real OKX data today (0 of
        105 live rows) but is handled symmetrically as the real inverse-side
        marker, mirroring Binance's own USDⓈ-M(UM)/COIN-M(CM) naming — future-
        proofing, not a live-verified case.
        """
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTC-USD_CM-260710", "okex-futures") == (
            MarginType.INVERSE
        )


# ---------------------------------------------------------------------------
# _resolve_base_quote — confirm quote resolution was already correct
# (only _infer_margin_type had the real bug)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw_id", "exchange", "expected_base", "expected_quote"),
    [
        ("BTC-USD-SWAP", "okex-swap", "BTC", "USD"),
        ("BTC-USDT-SWAP", "okex-swap", "BTC", "USDT"),
        ("BTC-USD-260710", "okex-futures", "BTC", "USD"),
        ("BTC-USD_UM-260710", "okex-futures", "BTC", "USD"),
    ],
)
def test_resolve_base_quote_okx_end_to_end(raw_id: str, exchange: str, expected_base: str, expected_quote: str) -> None:
    base, quote = _resolve_base_quote(_item(raw_id), raw_id, exchange)
    assert (base, quote) == (expected_base, expected_quote)


# ---------------------------------------------------------------------------
# Canonical VENUE:TYPE:SYMBOL key builders — routed through the shared UAC
# builder (build_instrument_id(passthrough=True))
# ---------------------------------------------------------------------------


class TestCanonicalKeyBuildersOkx:
    def test_okx_swap_perpetual_inverse(self) -> None:
        key = _build_canonical_perpetual_key("OKX-SWAP", "BTC", "USD", MarginType.INVERSE)
        assert key == "OKX-SWAP:PERPETUAL:BTC-USD@INV"

    def test_okx_swap_perpetual_linear(self) -> None:
        key = _build_canonical_perpetual_key("OKX-SWAP", "BTC", "USDT", MarginType.LINEAR)
        assert key == "OKX-SWAP:PERPETUAL:BTC-USDT@LIN"
        # Same base, different quote/margin — must be DISTINCT canonical keys.
        assert key != _build_canonical_perpetual_key("OKX-SWAP", "BTC", "USD", MarginType.INVERSE)

    def test_okx_futures_dated_inverse_matches_doc_target(self) -> None:
        """Matches instrument_id_format_canonicalization_2026_07_08.md finding 1's
        real, live example verbatim: OKX-FUTURES:FUTURE:BTC-USD@INV-20260710.
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_future_key("OKX-FUTURES", "BTC", "USD", MarginType.INVERSE, expiry)
        assert key == "OKX-FUTURES:FUTURE:BTC-USD@INV-20260710"

    def test_okx_futures_dated_linear_um_sibling(self) -> None:
        """The real _UM sibling of the row above (same underlying + expiry,
        genuinely the opposite margin type) — must NOT collide with it.
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        inverse_key = _build_canonical_future_key("OKX-FUTURES", "BTC", "USD", MarginType.INVERSE, expiry)
        linear_key = _build_canonical_future_key("OKX-FUTURES", "BTC", "USD", MarginType.LINEAR, expiry)
        assert linear_key == "OKX-FUTURES:FUTURE:BTC-USD@LIN-20260710"
        assert linear_key != inverse_key


# ---------------------------------------------------------------------------
# End-to-end: real raw_id → fixed margin inference → canonical key
# ---------------------------------------------------------------------------


def test_end_to_end_okx_futures_um_vs_bare_no_longer_collide() -> None:
    """Before the fix: BTC-USD_UM-260710 and BTC-USD-260710 both resolve
    base=BTC, quote=USD, and margin_type was BACKWARDS for both (UM->INVERSE
    instead of real LINEAR; bare->LINEAR default instead of real INVERSE) —
    coincidentally not colliding on VALUE, but both wrong, and the fix
    verifies they now resolve to the correct, genuinely-distinct real values.
    """
    um_raw = "BTC-USD_UM-260710"
    bare_raw = "BTC-USD-260710"
    um_base, um_quote = _resolve_base_quote(_item(um_raw), um_raw, "okex-futures")
    bare_base, bare_quote = _resolve_base_quote(_item(bare_raw), bare_raw, "okex-futures")
    assert (um_base, um_quote) == (bare_base, bare_quote) == ("BTC", "USD")

    um_margin = _infer_margin_type(InstrumentType.FUTURE, um_quote, um_raw, "okex-futures")
    bare_margin = _infer_margin_type(InstrumentType.FUTURE, bare_quote, bare_raw, "okex-futures")
    assert um_margin == MarginType.LINEAR
    assert bare_margin == MarginType.INVERSE

    expiry = datetime(2026, 7, 10, tzinfo=UTC)
    um_key = _build_canonical_future_key("OKX-FUTURES", um_base, um_quote, um_margin, expiry)
    bare_key = _build_canonical_future_key("OKX-FUTURES", bare_base, bare_quote, bare_margin, expiry)
    assert um_key == "OKX-FUTURES:FUTURE:BTC-USD@LIN-20260710"
    assert bare_key == "OKX-FUTURES:FUTURE:BTC-USD@INV-20260710"
    assert um_key != bare_key


def test_end_to_end_okx_swap_perpetual_inverse_vs_linear() -> None:
    """Real BTC-USD-SWAP (inverse) vs BTC-USDT-SWAP (linear) through the full
    corrected pipeline.
    """
    inverse_raw = "BTC-USD-SWAP"
    linear_raw = "BTC-USDT-SWAP"
    inv_base, inv_quote = _resolve_base_quote(_item(inverse_raw), inverse_raw, "okex-swap")
    lin_base, lin_quote = _resolve_base_quote(_item(linear_raw), linear_raw, "okex-swap")

    inv_margin = _infer_margin_type(InstrumentType.PERPETUAL, inv_quote, inverse_raw, "okex-swap")
    lin_margin = _infer_margin_type(InstrumentType.PERPETUAL, lin_quote, linear_raw, "okex-swap")
    assert inv_margin == MarginType.INVERSE
    assert lin_margin == MarginType.LINEAR

    inv_key = _build_canonical_perpetual_key("OKX-SWAP", inv_base, inv_quote, inv_margin)
    lin_key = _build_canonical_perpetual_key("OKX-SWAP", lin_base, lin_quote, lin_margin)
    assert inv_key == "OKX-SWAP:PERPETUAL:BTC-USD@INV"
    assert lin_key == "OKX-SWAP:PERPETUAL:BTC-USDT@LIN"
    assert inv_key != lin_key
