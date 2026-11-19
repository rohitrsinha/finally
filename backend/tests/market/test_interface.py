"""Tests for normalize_ticker()."""

import pytest

from app.market import normalize_ticker


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("aapl", "AAPL"), ("  TSLA ", "TSLA"), ("brk.b", "BRK.B"), ("BF-B", "BF-B"), ("V", "V")],
)
def test_normalizes_valid_tickers(raw, expected):
    assert normalize_ticker(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "$$$", "1ABC", "AAPL MSFT", "ABCDEFGHIJK", "A/B", "A-", "A.", "A.B.C", "BRK..B"],
)
def test_rejects_invalid_tickers(raw):
    with pytest.raises(ValueError):
        normalize_ticker(raw)


@pytest.mark.parametrize("raw", [None, 123, 1.5, ["AAPL"], {"ticker": "AAPL"}])
def test_rejects_non_string_with_value_error(raw):
    """3.6: LLM output can contain null/numeric tickers; API routes catch ValueError."""
    with pytest.raises(ValueError):
        normalize_ticker(raw)
