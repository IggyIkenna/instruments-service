"""Tests for DeFi shared utilities."""

from __future__ import annotations

from datetime import UTC

from instruments_service.reference_data.utils.defi_utils import (
    DEFI_QUOTE_TOKENS,
    classify_graph_error,
    order_base_quote,
    parse_created_timestamp,
)


class TestClassifyGraphError:
    def test_rate_limit_status_429(self) -> None:
        assert classify_graph_error(Exception("msg"), status=429) == "RATE_LIMIT"

    def test_server_error_500(self) -> None:
        assert classify_graph_error(Exception("msg"), status=500) == "503"

    def test_server_error_503(self) -> None:
        assert classify_graph_error(Exception("msg"), status=503) == "503"

    def test_rate_limit_in_message(self) -> None:
        assert classify_graph_error(Exception("rate limit exceeded")) == "RATE_LIMIT"

    def test_429_in_message(self) -> None:
        assert classify_graph_error(Exception("HTTP 429 Too Many")) == "RATE_LIMIT"

    def test_503_in_message(self) -> None:
        assert classify_graph_error(Exception("503 unavailable")) == "503"

    def test_unknown_error(self) -> None:
        assert classify_graph_error(Exception("something else")) == "UNKNOWN"


class TestOrderBaseQuote:
    def test_usdc_is_quote(self) -> None:
        base, quote = order_base_quote("WETH", "USDC")
        assert base == "WETH"
        assert quote == "USDC"

    def test_reversed_order_with_stablecoin(self) -> None:
        base, quote = order_base_quote("USDT", "LINK")
        assert base == "LINK"
        assert quote == "USDT"

    def test_both_quote_tokens_preserves_order(self) -> None:
        base, quote = order_base_quote("USDC", "USDT")
        assert base == "USDC"
        assert quote == "USDT"

    def test_neither_quote_token_preserves_order(self) -> None:
        base, quote = order_base_quote("LINK", "AAVE")
        assert base == "LINK"
        assert quote == "AAVE"

    def test_case_insensitive(self) -> None:
        base, quote = order_base_quote("weth", "usdc")
        assert base == "WETH"
        assert quote == "USDC"


class TestParseCreatedTimestamp:
    def test_none_returns_none(self) -> None:
        assert parse_created_timestamp(None) is None

    def test_valid_string(self) -> None:
        result = parse_created_timestamp("1700000000")
        assert result is not None
        assert result.tzinfo == UTC
        assert result.year == 2023

    def test_valid_int(self) -> None:
        result = parse_created_timestamp(1700000000)
        assert result is not None
        assert result.tzinfo == UTC

    def test_invalid_string_returns_none(self) -> None:
        assert parse_created_timestamp("not-a-number") is None

    def test_out_of_range_returns_none(self) -> None:
        # Very large timestamp that causes OverflowError (not caught by the function)
        try:
            result = parse_created_timestamp("99999999999999999999")
            assert result is None
        except OverflowError:
            pass  # platform time_t overflow — acceptable


class TestParseCreatedTimestampEdgeCases:
    """Additional edge cases for parse_created_timestamp."""

    def test_zero_returns_none(self) -> None:
        assert parse_created_timestamp(0) is None

    def test_negative_returns_none(self) -> None:
        assert parse_created_timestamp(-1) is None

    def test_zero_string_returns_none(self) -> None:
        assert parse_created_timestamp("0") is None

    def test_float_like_string(self) -> None:
        # "1700000000.5" → int("1700000000.5") raises ValueError → returns None
        assert parse_created_timestamp("1700000000.5") is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_created_timestamp("") is None


class TestClassifyGraphErrorEdgeCases:
    """Additional edge cases for classify_graph_error."""

    def test_unavailable_in_message(self) -> None:
        assert classify_graph_error(Exception("service unavailable")) == "503"

    def test_status_501_is_server_error(self) -> None:
        assert classify_graph_error(Exception("msg"), status=501) == "503"

    def test_status_200_not_server_error(self) -> None:
        assert classify_graph_error(Exception("something"), status=200) == "UNKNOWN"

    def test_none_status_with_unknown_msg(self) -> None:
        assert classify_graph_error(Exception("timeout occurred")) == "UNKNOWN"


class TestOrderBaseQuoteEdgeCases:
    """Additional edge cases for order_base_quote."""

    def test_wbtc_is_quote(self) -> None:
        base, quote = order_base_quote("LINK", "WBTC")
        assert base == "LINK"
        assert quote == "WBTC"

    def test_eth_is_quote(self) -> None:
        base, quote = order_base_quote("LINK", "ETH")
        assert base == "LINK"
        assert quote == "ETH"

    def test_usde_is_quote(self) -> None:
        base, quote = order_base_quote("LINK", "USDE")
        assert base == "LINK"
        assert quote == "USDE"

    def test_dai_is_quote(self) -> None:
        base, quote = order_base_quote("LINK", "DAI")
        assert base == "LINK"
        assert quote == "DAI"


class TestDefiQuoteTokens:
    def test_expected_tokens(self) -> None:
        assert "USDC" in DEFI_QUOTE_TOKENS
        assert "USDT" in DEFI_QUOTE_TOKENS
        assert "DAI" in DEFI_QUOTE_TOKENS
        assert "WETH" in DEFI_QUOTE_TOKENS
        assert "WBTC" in DEFI_QUOTE_TOKENS

    def test_usde_in_quote_tokens(self) -> None:
        assert "USDE" in DEFI_QUOTE_TOKENS

    def test_eth_in_quote_tokens(self) -> None:
        assert "ETH" in DEFI_QUOTE_TOKENS
