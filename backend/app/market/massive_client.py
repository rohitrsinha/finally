"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def to_unix_seconds(ts: float | int | None) -> float | None:
    """Convert a Massive timestamp to Unix seconds.

    Massive mixes units: trade timestamps (``sip_timestamp``, ``updated``) are
    nanoseconds, minute-bar timestamps are milliseconds. Detect by magnitude.
    """
    if not ts:
        return None
    ts = float(ts)
    if ts > 1e17:  # nanoseconds
        return ts / 1e9
    if ts > 1e14:  # microseconds
        return ts / 1e6
    if ts > 1e11:  # milliseconds
        return ts / 1e3
    return ts


def _positive_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def parse_snapshot(snap: Any) -> tuple[str, float, float] | None:
    """Extract ``(ticker, price, timestamp_seconds)`` from a massive TickerSnapshot.

    Uses the real SDK field names (``last_trade.price`` / ``last_trade.sip_timestamp``).
    Falls back to the minute bar, the day bar, then the previous day's close when
    no last trade is available. Returns None if the snapshot has no usable price.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker or not isinstance(ticker, str):
        return None

    last_trade = getattr(snap, "last_trade", None)
    minute = getattr(snap, "min", None)
    day = getattr(snap, "day", None)
    prev_day = getattr(snap, "prev_day", None)

    candidates = [
        (getattr(last_trade, "price", None), getattr(last_trade, "sip_timestamp", None)),
        (getattr(minute, "close", None), getattr(minute, "timestamp", None)),
        (getattr(day, "close", None), None),
        (getattr(prev_day, "close", None), None),
    ]
    for raw_price, raw_ts in candidates:
        price = _positive_float(raw_price)
        if price is None:
            continue
        timestamp = (
            to_unix_seconds(raw_ts)
            or to_unix_seconds(getattr(snap, "updated", None))
            or time.time()
        )
        return ticker.upper(), price, timestamp
    return None


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                parsed = parse_snapshot(snap)
                if parsed is None:
                    logger.warning(
                        "Skipping snapshot for %s: no usable price",
                        getattr(snap, "ticker", "???"),
                    )
                    continue
                ticker, price, timestamp = parsed
                self._cache.update(ticker=ticker, price=price, timestamp=timestamp)
                processed += 1
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
