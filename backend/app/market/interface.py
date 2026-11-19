"""Abstract interface for market data sources."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

# A letter, then letters/digits, optionally one '.' or '-' class suffix (BRK.B, BF-B)
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:[.\-][A-Z0-9]+)?$")
_MAX_TICKER_LEN = 10


def normalize_ticker(raw: object) -> str:
    """Upper-case and strip a ticker symbol; raise ValueError if it is invalid.

    Used by both data sources and by the API layer, so 'tsla ', 'TSLA' and 'Tsla'
    are the same ticker everywhere. Accepts class shares such as 'BRK.B' and 'BF-B'.
    Non-string input (e.g. a null ticker from LLM output) also raises ValueError.
    """
    if not isinstance(raw, str):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    ticker = raw.strip().upper()
    if len(ticker) > _MAX_TICKER_LEN or not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Tickers added or removed before start() are applied on top of `tickers`.
        Must be called at most once per instance: a second call raises
        RuntimeError (create a new source instead).
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker. May be called before
        start(); the ticker is then tracked from the moment start() runs.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""

    @property
    def name(self) -> str:
        """Human-readable source name for /api/health and logs."""
        return type(self).__name__
