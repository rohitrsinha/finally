"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    """The fields FinAlly needs from one Massive ticker snapshot."""

    ticker: str
    price: float
    timestamp: float  # Unix seconds
    reference_price: float | None  # previous close; baseline for daily change


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


def parse_snapshot(snap: Any) -> ParsedQuote | None:
    """Extract a ParsedQuote from a massive TickerSnapshot.

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
        return ParsedQuote(
            ticker=ticker.upper(),
            price=price,
            timestamp=timestamp,
            reference_price=_positive_float(getattr(prev_day, "close", None)),
        )
    return None


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    poll_interval  normal cadence (free tier, 5 req/min: 15s; paid: 2-5s)
    min_gap        minimum spacing between any two requests. An add_ticker()
                   wake-up never polls sooner than this after the previous
                   poll (default: min(poll_interval, 12s), i.e. <= 5 req/min)
    max_backoff    cap for exponential backoff after consecutive failures
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_gap: float | None = None,
        max_backoff: float = 120.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_gap = min(poll_interval, 12.0) if min_gap is None else min_gap
        self._max_backoff = max_backoff
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._started = False
        self._client: RESTClient | None = None
        self._wake = asyncio.Event()
        self._last_poll = 0.0  # event-loop clock time of the last request
        self._failures = 0  # consecutive failed polls
        self._missing: set[str] = set()  # tracked tickers the API returned nothing for

    async def start(self, tickers: list[str]) -> None:
        if self._started:
            raise RuntimeError("MassiveDataSource.start() called twice")
        self._started = True
        self._client = RESTClient(api_key=self._api_key, connect_timeout=5.0, read_timeout=10.0)
        # Normalize and de-duplicate while keeping order; include pre-start adds
        self._tickers = list(
            dict.fromkeys([*(normalize_ticker(t) for t in tickers), *self._tickers])
        )

        # Do an immediate first poll so the cache has data right away. It covers
        # any pre-start add_ticker() calls, so drop their wake-up request.
        self._wake.clear()
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
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
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()  # request an early poll; min_gap still applies
            logger.info("Massive: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._missing.discard(ticker)
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    @property
    def unpriced_tickers(self) -> set[str]:
        """Tracked tickers the last poll returned no usable data for (likely invalid)."""
        return set(self._missing)

    # --- Internal ---

    def _current_delay(self) -> float:
        """Normal interval, doubled per consecutive failure, capped at max_backoff."""
        if self._failures == 0:
            return self._interval
        return min(self._interval * 2**self._failures, self._max_backoff)

    async def _poll_loop(self) -> None:
        """Poll on interval (or earlier when woken). First poll happened in start()."""
        while True:
            if not self._tickers:
                # Nothing to poll: sleep until add_ticker() wakes us. Without this,
                # a stale _last_poll makes the timed wait below return instantly
                # and the loop spins at 100% CPU.
                await self._wake.wait()
            await self._wait_for_next_poll()
            self._wake.clear()
            await self._poll_once()

    async def _wait_for_next_poll(self) -> None:
        loop = asyncio.get_running_loop()
        due = self._last_poll + self._current_delay()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=max(0.0, due - loop.time()))
        except TimeoutError:
            return  # regular cadence
        # Woken early by add_ticker(): still respect the minimum request gap and,
        # while the API is failing, the backoff (don't hammer a 401/429).
        gap = self._min_gap if self._failures == 0 else max(self._min_gap, self._current_delay())
        earliest = self._last_poll + gap
        await asyncio.sleep(max(0.0, earliest - loop.time()))

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        tickers = list(self._tickers)  # the list may change while the fetch is in flight
        self._last_poll = asyncio.get_running_loop().time()
        try:
            # The Massive RESTClient is synchronous: run it in a thread so the
            # event loop isn't blocked. Cache writes happen back on the loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as e:
            # Don't re-raise: the loop retries after a backoff.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.
            self._failures += 1
            logger.error(
                "Massive poll failed (%d in a row, next attempt in %.0fs): %s",
                self._failures,
                self._current_delay(),
                e,
            )
            return

        self._failures = 0
        tracked = set(self._tickers)  # re-read: drop tickers removed mid-flight
        seen: set[str] = set()
        for snap in snapshots or []:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.warning(
                    "Skipping snapshot for %s: no usable price",
                    getattr(snap, "ticker", "???"),
                )
                continue
            if quote.ticker not in tracked:
                continue
            self._cache.update(
                ticker=quote.ticker,
                price=quote.price,
                timestamp=quote.timestamp,
                reference_price=quote.reference_price,
            )
            seen.add(quote.ticker)

        self._missing = (set(tickers) & tracked) - seen
        if self._missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(self._missing)))
        logger.debug("Massive poll: updated %d/%d tickers", len(seen), len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
