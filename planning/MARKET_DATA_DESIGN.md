# Market Data Backend — Detailed Design

**Scope:** everything under `backend/app/market/`: the unified data-source interface, the shared price cache, the GBM simulator, the Massive (Polygon.io) REST poller, the source factory, the SSE stream endpoint, and how the rest of the backend (watchlist, portfolio, trades, chat) connects to it.

**Status of this document:** This is the reference design. A first version of the subsystem already exists in `backend/app/market/` (summarized in `MARKET_DATA_SUMMARY.md`). This design keeps that module layout and public API. It also specifies a set of **required changes** (listed in §0), including one blocking bug in the Massive client that the current mocked tests don't catch. Every code block below is the target implementation.

---

## Table of Contents

0. [Changes vs. the current implementation](#0-changes-vs-the-current-implementation)
1. [Architecture](#1-architecture)
2. [Module layout & public API](#2-module-layout--public-api)
3. [Data model: `models.py`](#3-data-model-modelspy)
4. [Price cache: `cache.py`](#4-price-cache-cachepy)
5. [Unified interface: `interface.py`](#5-unified-interface-interfacepy)
6. [Seed data: `seed_prices.py`](#6-seed-data-seed_pricespy)
7. [GBM simulator: `simulator.py`](#7-gbm-simulator-simulatorpy)
8. [Massive API client: `massive_client.py`](#8-massive-api-client-massive_clientpy)
9. [Factory & configuration: `factory.py`](#9-factory--configuration-factorypy)
10. [SSE streaming: `stream.py`](#10-sse-streaming-streampy)
11. [FastAPI integration](#11-fastapi-integration)
12. [Coordinating with the watchlist, portfolio and chat](#12-coordinating-with-the-watchlist-portfolio-and-chat)
13. [Testing](#13-testing)
14. [Failure modes & edge cases](#14-failure-modes--edge-cases)
15. [Frontend contract (SSE payload)](#15-frontend-contract-sse-payload)

---

## 0. Changes vs. the current implementation

| # | Area | Current behaviour | Design | Severity |
|---|------|-------------------|--------|----------|
| C1 | `massive_client.py` | Reads `snap.last_trade.timestamp`. The real `massive` `LastTrade` model has **no `timestamp` attribute** (it has `sip_timestamp`, in **nanoseconds**), so every snapshot raises `AttributeError` and is skipped. With a real API key the cache stays empty. Unit tests pass only because they use `MagicMock` snapshots. | Parse snapshots with a dedicated `parse_snapshot()` helper that uses the real model fields, tolerates missing parts, and normalizes timestamp units (§8.3). Test against real `TickerSnapshot.from_dict(...)` objects (§13.4). | **Blocking** |
| C2 | `models.py` / `cache.py` | `change_percent` is tick-over-tick only. The watchlist needs **daily change %** (PLAN §10). | Add `reference_price` (previous close for Massive, first-seen price for the simulator) and `day_change` / `day_change_percent` properties (§3, §4). | High |
| C3 | `stream.py` | A module-level `router` is reused, so calling `create_stream_router()` twice registers `/prices` twice. There is no heartbeat, so with Massive's 15s cadence the connection can sit idle and get closed by proxies. | Build a new router inside the factory. Send a `: ping` comment when idle. Include `id:` lines (§10). | Medium |
| C4 | `massive_client.py` | A newly added ticker has no price until the next poll (up to 15s). There is no backoff on repeated failures. The poll interval can't be configured. | Wake the poll loop early on `add_ticker` while respecting a rate-limit gap. Use exponential backoff on failure. Add a `MASSIVE_POLL_INTERVAL` env var (§8.4, §9). | Medium |
| C5 | `simulator.py` / sources | Ticker symbols are not normalized in the simulator, so `"tsla"` and `"TSLA"` become two different tickers. | Add a shared `normalize_ticker()` used by both sources (§5). | Low |
| C6 | `cache.py` | `timestamp or time.time()` treats `0.0` as missing. `version` is read without the lock. | Use `timestamp if timestamp is not None else time.time()` and read `version` under the lock (§4). | Low |
| C7 | `simulator.py` | Global `np.random` / `random` make tests non-deterministic. | Inject `np.random.Generator` with an optional `seed` (§7). | Low |

The public surface (`PriceUpdate`, `PriceCache`, `MarketDataSource`, `create_market_data_source`, `create_stream_router`) doesn't change. Every addition is backward compatible: new fields have defaults, and new constructor parameters are optional.

---

## 1. Architecture

```
                    ┌──────────────────────────────┐
                    │  create_market_data_source() │  reads MASSIVE_API_KEY
                    └──────────────┬───────────────┘
                                   │ returns one of
             ┌─────────────────────┴──────────────────────┐
             ▼                                            ▼
┌───────────────────────────┐              ┌───────────────────────────────┐
│ SimulatorDataSource       │              │ MassiveDataSource             │
│  asyncio task, 500ms tick │              │  asyncio task, 15s poll       │
│  GBMSimulator.step()      │              │  REST snapshot (in a thread)  │
└─────────────┬─────────────┘              └───────────────┬───────────────┘
              │ cache.update(...)                          │ cache.update(...)
              └──────────────────┬─────────────────────────┘
                                 ▼
                 ┌───────────────────────────────┐
                 │ PriceCache (thread-safe)      │  latest PriceUpdate per ticker
                 │  + monotonic version counter  │
                 └──┬──────────────┬─────────────┘
                    │              │
       ┌────────────┘              └───────────────────────┐
       ▼                                                   ▼
┌───────────────────────┐              ┌──────────────────────────────────────┐
│ GET /api/stream/prices│              │ Portfolio valuation, trade execution,│
│ SSE, pushes on change │              │ watchlist API, LLM context, snapshots│
└───────────────────────┘              └──────────────────────────────────────┘
```

**Core rules**

1. **One producer, many readers.** Exactly one `MarketDataSource` runs per process and it is the only writer to the `PriceCache`. Everything else only reads.
2. **Sources push and never return prices.** The interface has no `get_price()`. Consumers always read the cache. That way the SSE stream, trade fills and portfolio valuation all see the same price.
3. **Source-agnostic downstream.** No code outside `app/market/` imports `simulator` or `massive_client`.
4. **The tracked set is watchlist ∪ open positions.** The market data layer doesn't know about the database. The API layer decides which tickers are tracked and calls `add_ticker` / `remove_ticker` (§12).

---

## 2. Module layout & public API

```
backend/app/market/
├── __init__.py         # public re-exports
├── models.py           # PriceUpdate (frozen dataclass)
├── cache.py            # PriceCache
├── interface.py        # MarketDataSource ABC + normalize_ticker()
├── seed_prices.py      # seed prices, GBM params, correlation groups
├── simulator.py        # GBMSimulator (math) + SimulatorDataSource (async wrapper)
├── massive_client.py   # MassiveDataSource + parse_snapshot()
├── factory.py          # create_market_data_source() + MarketDataConfig
└── stream.py           # create_stream_router() (SSE)
```

```python
# backend/app/market/__init__.py
"""Market data subsystem for FinAlly.

Downstream code imports only from here:

    from app.market import PriceCache, PriceUpdate, MarketDataSource, \
        create_market_data_source, create_stream_router, normalize_ticker
"""

from .cache import PriceCache
from .factory import MarketDataConfig, create_market_data_source
from .interface import MarketDataSource, normalize_ticker
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "MarketDataConfig",
    "normalize_ticker",
    "create_market_data_source",
    "create_stream_router",
]
```

Dependencies (already in `backend/pyproject.toml`): `fastapi`, `uvicorn[standard]`, `numpy>=2`, `massive>=1.0`. Dev: `pytest`, `pytest-asyncio` (`asyncio_mode = "auto"`), `httpx` (add it for the SSE route test in §13.5).

---

## 3. Data model: `models.py`

`PriceUpdate` is the only type that leaves the market data layer.

```python
# backend/app/market/models.py
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

Direction = Literal["up", "down", "flat"]


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time.

    price            latest price
    previous_price   price at the previous update (drives flash up/down)
    timestamp        Unix seconds (float) when the price was observed
    reference_price  baseline for "daily change": previous close (Massive)
                     or the first price seen this session (simulator)
    """

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)
    reference_price: float | None = None

    # --- tick-over-tick (drives the green/red flash) ---

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> Direction:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- daily (drives the watchlist "change %" column) ---

    @property
    def day_change(self) -> float | None:
        if self.reference_price is None:
            return None
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float | None:
        if not self.reference_price:  # None or 0
            return None
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Single serialization point for SSE and REST responses."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

**Why derived properties?** `direction`, `change` and the day fields are computed from the stored prices, so they can never disagree with them. The object is frozen, so it's safe to share across tasks and threads without copying.

**Example**

```python
>>> u = PriceUpdate("AAPL", price=191.20, previous_price=191.05,
...                 timestamp=1763460000.0, reference_price=190.00)
>>> u.direction, u.change, u.day_change_percent
('up', 0.15, 0.6316)
```

---

## 4. Price cache: `cache.py`

```python
# backend/app/market/cache.py
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest PriceUpdate per ticker, plus a version counter.

    Writer: the single active MarketDataSource (event loop thread; the Massive
            fetch itself runs in a worker thread but writes happen back on the loop).
    Readers: SSE generator, portfolio/trade/watchlist routes, snapshot task.
    A threading.Lock (not asyncio.Lock) is used so the cache is also safe to
    touch from worker threads (asyncio.to_thread) and sync code.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        - previous_price = last cached price, or `price` on first sight (flat).
        - reference_price = explicit arg, else the previously cached reference,
          else `price` on first sight. So the simulator never passes it and
          gets "change since session start"; Massive passes previous close.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=timestamp if timestamp is not None else time.time(),
                reference_price=round(reference_price, 2) if reference_price else None,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy. Safe to iterate without holding the lock."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # so SSE clients see the removal

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: object) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Version counter.** The SSE loop wakes every 500ms and compares `cache.version` with the last version it sent. If nothing changed (common with Massive's 15s polls), it sends nothing. `remove()` also bumps the version so the next frame drops the ticker.

**Memory.** O(number of tracked tickers). The cache holds no history: sparklines are built on the frontend from the stream, and portfolio history lives in `portfolio_snapshots`.

---

## 5. Unified interface: `interface.py`

```python
# backend/app/market/interface.py
"""Abstract interface for market data sources."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Upper-case + strip. Raises ValueError for obviously invalid symbols.

    Used by both sources AND by the API layer before touching the DB, so
    'tsla ', 'TSLA' and 'Tsla' are the same ticker everywhere.
    Accepts class shares like 'BRK.B' and 'BF-B'.
    """
    ticker = raw.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations write PriceUpdates into a shared PriceCache on their own
    schedule. Consumers never ask the source for prices; they read the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])   # exactly once
        await source.add_ticker("TSLA")          # idempotent
        await source.remove_ticker("GOOGL")      # idempotent, also evicts from cache
        await source.stop()                      # idempotent
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Start the background task. Should seed the cache before returning
        when that is cheap (simulator: always; Massive: one initial poll)."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and remove it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (a copy)."""

    @property
    def name(self) -> str:
        """Human-readable source name for /api/health and logs."""
        return type(self).__name__
```

**Why async methods?** `add_ticker` on the Massive source may wake a network poll, and future sources (WebSocket feeds) need to `await` subscriptions. Using async everywhere keeps call sites the same regardless of the source.

---

## 6. Seed data: `seed_prices.py`

No change from the current implementation. It is reproduced here so this design is complete.

```python
# backend/app/market/seed_prices.py
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

# sigma = annualized volatility, mu = annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},
    "NVDA": {"sigma": 0.40, "mu": 0.08},
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},
    "V": {"sigma": 0.17, "mu": 0.04},
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3   # cross-sector and unknown tickers
TSLA_CORR = 0.3          # TSLA moves on its own
```

A ticker added at runtime that isn't in `SEED_PRICES` starts at a random price between $50 and $300 and uses `DEFAULT_PARAMS`.

**Correlation matrix is always positive-definite.** Every off-diagonal value is either 0.6 (within one block), 0.5 (within the other block) or 0.3 (everything else), and the diagonal is 1. That is 0.3·J plus block terms plus a positive diagonal, which is PD for any n. So `np.linalg.cholesky` won't fail as tickers are added. A test checks this with 50 tickers (§13.2).

---

## 7. GBM simulator: `simulator.py`

### 7.1 The math

Each tick, every price follows exact-discretization geometric Brownian motion:

```
S(t+Δt) = S(t) · exp( (μ − σ²/2)·Δt + σ·√Δt·Z )
```

- `Δt = 0.5 s / (252 days × 6.5 h × 3600 s) ≈ 8.48e-8` (fraction of a trading year)
- `Z = L · ε`, where `ε ~ N(0, I)` and `L = cholesky(C)` for the sector correlation matrix `C`
- Prices stay positive because of the exponential.

Per-tick standard deviation is `σ·√Δt`. For AAPL (σ = 0.22) that's about 0.0064%, or roughly 1.2¢ per tick at $190. Over one simulated hour (7,200 ticks) it grows to about 0.55%, which is lively without being silly.

**Shock events.** Each tick, each ticker has probability `p = 0.001` of jumping ±2–5%. With 10 tickers at 2 ticks/s, that's one event about every 50s somewhere on the board.

### 7.2 Implementation

```python
# backend/app/market/simulator.py
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS, INTRA_FINANCE_CORR,
    INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Pure, synchronous price engine. No asyncio, no cache. Easy to unit test."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._dt = dt
        self._sqrt_dt = math.sqrt(dt)
        self._event_prob = event_probability
        self._rng = np.random.default_rng(seed)

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for t in tickers:
            self._add_internal(t)
        self._rebuild_cholesky()

    # ---- public ----

    def step(self) -> dict[str, float]:
        """Advance every ticker by one Δt. Returns {ticker: new_price}. Hot path."""
        n = len(self._tickers)
        if n == 0:
            return {}

        eps = self._rng.standard_normal(n)
        z = self._cholesky @ eps if self._cholesky is not None else eps
        shocks = self._rng.random(n) < self._event_prob

        out: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            p = self._params[ticker]
            mu, sigma = p["mu"], p["sigma"]
            log_ret = (mu - 0.5 * sigma * sigma) * self._dt + sigma * self._sqrt_dt * z[i]
            price = self._prices[ticker] * math.exp(log_ret)

            if shocks[i]:
                magnitude = self._rng.uniform(0.02, 0.05)
                sign = 1.0 if self._rng.random() < 0.5 else -1.0
                price *= 1.0 + sign * magnitude
                logger.debug("Event on %s: %+.1f%%", ticker, sign * magnitude * 100)

            self._prices[ticker] = price
            out[ticker] = round(price, 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---- internals ----

    def _add_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, float(self._rng.uniform(50.0, 300.0)))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech, fin = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in fin and t2 in fin:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """Runs GBMSimulator.step() every `update_interval` seconds into the cache."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._seed = seed
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        normalized = list(dict.fromkeys(normalize_ticker(t) for t in tickers))  # dedupe, keep order
        self._sim = GBMSimulator(normalized, event_probability=self._event_prob, seed=self._seed)
        for t in normalized:  # seed cache so the first SSE frame is complete
            self._cache.update(t, self._sim.get_price(t))
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalized))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._cache.update(ticker, self._sim.get_price(ticker))  # priced immediately
        logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker, price)
            except Exception:  # never let the loop die
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Concurrency note.** `add_ticker`, `remove_ticker` and `_run_loop` all run on the event loop thread, and `step()` never awaits. So the simulator's internal dicts are never modified in the middle of a step, and the simulator needs no lock.

**Performance.** One step with 10 tickers is one 10×10 mat-vec plus 10 `exp` calls, which takes microseconds. Rebuilding the Cholesky factor is O(n³) but only runs on add/remove with n < 50.

---

## 8. Massive API client: `massive_client.py`

### 8.1 Endpoint

One request fetches every tracked ticker. This matters on the free tier (5 requests/min).

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
Authorization: Bearer <MASSIVE_API_KEY>
```

Python SDK (`massive` package, synchronous, urllib3-based, retries 5xx 3 times):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key=key, connect_timeout=5.0, read_timeout=10.0)
snaps = client.get_snapshot_all(SnapshotMarketType.STOCKS, tickers=["AAPL", "MSFT"])
# -> list[massive.rest.models.TickerSnapshot]
```

### 8.2 Response shape and the real SDK model

Raw JSON for one ticker (keys abbreviated as the API returns them):

```json
{
  "ticker": "AAPL",
  "todaysChange": 2.32, "todaysChangePerc": 1.23,
  "updated": 1763460000123456789,
  "day":     {"o": 189.1, "h": 192.0, "l": 188.4, "c": 191.5, "v": 48211000, "vw": 190.7},
  "prevDay": {"o": 188.0, "h": 190.2, "l": 187.5, "c": 189.2, "v": 51000000, "vw": 189.1},
  "min":     {"o": 191.4, "h": 191.6, "l": 191.3, "c": 191.52, "v": 21000, "t": 1763459940000},
  "lastTrade": {"p": 191.52, "s": 100, "x": 4, "t": 1763460000123456789},
  "lastQuote": {"p": 191.51, "P": 191.53, "s": 3, "S": 5, "t": 1763460000120000000}
}
```

The SDK maps this into dataclasses. **These are the attribute names to use. They differ from the raw keys and from older notes in `archive/MASSIVE_API.md`:**

| Need | Attribute | Notes |
|------|-----------|-------|
| Latest price | `snap.last_trade.price` | `last_trade` may be `None` (halted, new listing, entitlement limits) |
| Trade time | `snap.last_trade.sip_timestamp` | **nanoseconds**; there is no `.timestamp` |
| Fallback price | `snap.min.close`, then `snap.day.close`, then `snap.prev_day.close` | `day` is zeroed pre-market |
| Fallback time | `snap.updated` (ns), `snap.min.timestamp` (ms) | |
| Previous close | `snap.prev_day.close` | baseline for daily change % |
| Daily change % | `snap.todays_change_percent` | not used; computed from `reference_price` instead so both sources behave the same |

### 8.3 Snapshot parsing (fixes C1)

```python
# backend/app/market/massive_client.py  (parsing helpers)
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float              # Unix seconds
    reference_price: float | None  # previous close


def to_unix_seconds(ts: float | int | None) -> float | None:
    """Massive mixes units: trades in ns, aggregates in ms. Detect by magnitude."""
    if not ts:
        return None
    ts = float(ts)
    if ts > 1e17:   # nanoseconds
        return ts / 1e9
    if ts > 1e14:   # microseconds
        return ts / 1e6
    if ts > 1e11:   # milliseconds
        return ts / 1e3
    return ts       # already seconds


def _positive(x: Any) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def parse_snapshot(snap: Any) -> ParsedQuote | None:
    """Turn a massive TickerSnapshot into a ParsedQuote, or None if unusable."""
    ticker = getattr(snap, "ticker", None)
    if not ticker:
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
        price = _positive(raw_price)
        if price is not None:
            ts = (
                to_unix_seconds(raw_ts)
                or to_unix_seconds(getattr(snap, "updated", None))
                or time.time()
            )
            return ParsedQuote(
                ticker=ticker.upper(),
                price=price,
                timestamp=ts,
                reference_price=_positive(getattr(prev_day, "close", None)),
            )
    return None
```

### 8.4 Data source

```python
# backend/app/market/massive_client.py  (continued)
import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint and writes results to the PriceCache.

    poll_interval  normal cadence (free tier: 15s; paid: 2-5s)
    min_gap        minimum spacing between ANY two requests; an add_ticker()
                   wake-up never polls sooner than this after the last poll
                   (free tier: 12s = 5 req/min)
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
        self._client: RESTClient | None = None
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._last_poll = 0.0        # time.monotonic() of last request
        self._failures = 0
        self._missing: set[str] = set()  # tickers the API returned nothing for

    # ---- lifecycle ----

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key, connect_timeout=5.0, read_timeout=10.0)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once()  # populate the cache before the app serves traffic
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

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
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        self._wake.set()  # ask for an early poll (rate-limit gap still applies)
        logger.info("Massive: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._missing.discard(ticker)
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    @property
    def unpriced_tickers(self) -> set[str]:
        """Tracked tickers the API returned no usable data for (likely invalid)."""
        return set(self._missing)

    # ---- loop ----

    def _current_delay(self) -> float:
        if self._failures == 0:
            return self._interval
        return min(self._interval * 2 ** self._failures, self._max_backoff)

    async def _poll_loop(self) -> None:
        while True:
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
        # Woken early by add_ticker: still respect the minimum request gap.
        earliest = self._last_poll + self._min_gap
        await asyncio.sleep(max(0.0, earliest - loop.time()))

    async def _poll_once(self) -> None:
        if not self._tickers or self._client is None:
            return
        tickers = list(self._tickers)  # snapshot: the list may change while we're in the thread
        self._last_poll = asyncio.get_running_loop().time()
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as exc:  # AuthError, BadResponse(429/403/5xx), network errors
            self._failures += 1
            logger.error("Massive poll failed (%d in a row, next in %.0fs): %s",
                         self._failures, self._current_delay(), exc)
            return

        self._failures = 0
        tracked = set(self._tickers)  # re-read: tickers removed mid-flight are dropped
        seen: set[str] = set()
        for snap in snapshots or []:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.warning("Unusable snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            if quote.ticker not in tracked:
                continue
            self._cache.update(quote.ticker, quote.price, quote.timestamp, quote.reference_price)
            seen.add(quote.ticker)

        self._missing = set(tickers) - seen
        if self._missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(self._missing)))
        logger.debug("Massive poll: %d/%d tickers updated", len(seen), len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking SDK call, run via asyncio.to_thread."""
        assert self._client is not None
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
```

**Timing used here:** `asyncio.get_running_loop().time()` is monotonic. It's also the clock `asyncio.sleep` uses, so the deadline math stays consistent.

**Cache writes happen on the event loop.** Only the HTTP call runs in the worker thread. `_poll_once` writes to the cache after `await` returns, so writes stay on the loop thread like the simulator's.

**Rate budget example (free tier).** With the default `poll_interval=15`, steady state is 4 requests/min. If the user adds a ticker 3s after a poll, the loop wakes and then waits until `last_poll + 12s`, so the gap stays ≥ 12s (≤ 5/min). The new ticker gets a price 12s after the previous poll instead of 15s.

**Market closed.** `last_trade.price` holds the last trade (it may be after-hours). Prices won't change between polls, so `direction` stays `flat`. The UI just shows a quiet board, and nothing needs special handling.

---

## 9. Factory & configuration: `factory.py`

```python
# backend/app/market/factory.py
"""Select and build the market data source from environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default


@dataclass(frozen=True)
class MarketDataConfig:
    massive_api_key: str = ""
    massive_poll_interval: float = 15.0
    simulator_interval: float = 0.5
    simulator_event_probability: float = 0.001
    simulator_seed: int | None = None

    @classmethod
    def from_env(cls) -> MarketDataConfig:
        seed = os.environ.get("SIMULATOR_SEED", "").strip()
        return cls(
            massive_api_key=os.environ.get("MASSIVE_API_KEY", "").strip(),
            massive_poll_interval=max(2.0, _float_env("MASSIVE_POLL_INTERVAL", 15.0)),
            simulator_interval=_float_env("SIMULATOR_INTERVAL", 0.5),
            simulator_event_probability=_float_env("SIMULATOR_EVENT_PROBABILITY", 0.001),
            simulator_seed=int(seed) if seed.isdigit() else None,
        )


def create_market_data_source(
    price_cache: PriceCache, config: MarketDataConfig | None = None
) -> MarketDataSource:
    """Return an UNSTARTED source. Caller must `await source.start(tickers)`."""
    config = config or MarketDataConfig.from_env()
    if config.massive_api_key:
        logger.info("Market data source: Massive API (poll every %.1fs)",
                    config.massive_poll_interval)
        return MassiveDataSource(
            api_key=config.massive_api_key,
            price_cache=price_cache,
            poll_interval=config.massive_poll_interval,
        )
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(
        price_cache=price_cache,
        update_interval=config.simulator_interval,
        event_probability=config.simulator_event_probability,
        seed=config.simulator_seed,
    )
```

| Variable | Default | Effect |
|----------|---------|--------|
| `MASSIVE_API_KEY` | empty | Set and non-empty → Massive. Otherwise → simulator. |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between polls (floor 2). Use 2–5 on paid tiers. |
| `SIMULATOR_INTERVAL` | `0.5` | Simulator tick in seconds. |
| `SIMULATOR_EVENT_PROBABILITY` | `0.001` | Per-ticker, per-tick shock probability. |
| `SIMULATOR_SEED` | unset | Integer → deterministic price paths (handy for E2E tests). |

Add the optional ones (commented out) to `.env.example`. Only `MASSIVE_API_KEY` is part of the documented contract in PLAN §5. The others are developer knobs.

---

## 10. SSE streaming: `stream.py`

### 10.1 Endpoint

`GET /api/stream/prices` → `text/event-stream`. Each frame is a JSON object keyed by ticker with a **full snapshot of all tracked tickers**. Full snapshots (rather than deltas) mean a reconnecting client is correct after one frame, and the payload is small: about 10 × 250 bytes.

### 10.2 Implementation (fixes C3)

```python
# backend/app/market/stream.py
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    """Build a NEW router per call (no module-level state)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames: data when the cache version changes, a comment ping when idle."""
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    yield "retry: 1000\n\n"  # EventSource reconnects 1s after a drop

    loop = asyncio.get_running_loop()
    last_version = -1
    last_sent = loop.time()
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                yield f"id: {version}\nevent: prices\ndata: {payload}\n\n"
                last_sent = loop.time()
            elif loop.time() - last_sent >= heartbeat:
                yield ": ping\n\n"  # comment line; ignored by EventSource, keeps proxies happy
                last_sent = loop.time()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

> **Named event.** Frames use `event: prices`, so the frontend must listen with `source.addEventListener("prices", ...)`. `onmessage` only receives unnamed events. If you'd rather have clients use `onmessage`, drop the `event:` line, but then choose one convention and keep it everywhere.

### 10.3 Wire example

```
retry: 1000

id: 1042
event: prices
data: {"AAPL":{"ticker":"AAPL","price":190.12,"previous_price":190.07,"timestamp":1763460000.51,"change":0.05,"change_percent":0.0263,"direction":"up","reference_price":190.0,"day_change":0.12,"day_change_percent":0.0632},"MSFT":{...}}

: ping

```

**Why poll the cache instead of pushing from the producer?** It's one simple loop per client, with no fan-out queues and no back-pressure handling. Readers are naturally rate-limited to 2 frames/s. The worst-case added latency is `interval` (500ms), which is the simulator's tick period anyway.

---

## 11. FastAPI integration

The cache has to exist before routers are included (the SSE router captures it), and the source has to start only once the event loop is running. An app factory with a lifespan handles both:

```python
# backend/app/main.py  (market-data-relevant parts)
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.market import MarketDataSource, PriceCache, create_market_data_source, create_stream_router
from app.db import init_db, tracked_tickers  # owned by the backend/db agent


def create_app() -> FastAPI:
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(init_db)                     # lazy schema + seed
        tickers = await asyncio.to_thread(tracked_tickers)   # watchlist ∪ open positions
        await source.start(tickers)
        snapshot_task = asyncio.create_task(snapshot_loop(app))  # portfolio_snapshots every 30s
        try:
            yield
        finally:
            snapshot_task.cancel()
            await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = price_cache
    app.state.market_source = source

    app.include_router(create_stream_router(price_cache))
    # app.include_router(portfolio_router); app.include_router(watchlist_router); ...
    # app.mount("/", StaticFiles(directory="static", html=True), name="static")  # LAST
    return app


app = create_app()


# ---- dependencies for route modules ----

def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

`tracked_tickers()` (DB layer) is expected to return:

```sql
SELECT ticker FROM watchlist WHERE user_id = 'default'
UNION
SELECT ticker FROM positions WHERE user_id = 'default' AND quantity > 0;
```

Health endpoint example:

```python
@app.get("/api/health")
async def health(request: Request):
    cache: PriceCache = request.app.state.price_cache
    source: MarketDataSource = request.app.state.market_source
    return {"status": "ok", "market_data": source.name, "tickers_priced": len(cache)}
```

Run it with `uvicorn app.main:app --host 0.0.0.0 --port 8000`. Use **one worker**: the cache and the source are per-process, and SQLite is single-writer.

---

## 12. Coordinating with the watchlist, portfolio and chat

The market layer is deliberately unaware of the database. These are the rules the API layer must follow.

### 12.1 Add to watchlist (`POST /api/watchlist`, LLM `watchlist_changes: add`)

```python
@router.post("/api/watchlist", status_code=201)
async def add_to_watchlist(body: AddTicker, cache=Depends(get_price_cache),
                           source=Depends(get_market_source)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(400, str(e))
    inserted = await asyncio.to_thread(db.add_watchlist, ticker)   # False if already present
    await source.add_ticker(ticker)                                 # idempotent
    update = cache.get(ticker)                                      # simulator: set; Massive: maybe None
    return {"ticker": ticker, "added": inserted,
            "price": update.to_dict() if update else None}
```

### 12.2 Remove from watchlist

Only stop tracking the ticker if it isn't held. Otherwise the position couldn't be valued.

```python
@router.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    ticker = normalize_ticker(ticker)
    removed = await asyncio.to_thread(db.remove_watchlist, ticker)
    if not removed:
        raise HTTPException(404, f"{ticker} is not on the watchlist")
    if not await asyncio.to_thread(db.has_open_position, ticker):
        await source.remove_ticker(ticker)
    return {"ticker": ticker, "removed": True}
```

### 12.3 Trades (`POST /api/portfolio/trade`, LLM `trades`)

The fill price always comes from the cache:

```python
async def resolve_fill_price(ticker: str, cache: PriceCache, source: MarketDataSource) -> float:
    price = cache.get_price(ticker)
    if price is None and ticker not in source.get_tickers():
        await source.add_ticker(ticker)          # simulator prices it immediately
        price = cache.get_price(ticker)
    if price is None:
        raise TradeError(f"No market price available for {ticker} yet; try again shortly")
    return price
```

After a trade:

- **Buy** of a ticker not on the watchlist: leave it tracked (it's now a position).
- **Sell** that closes a position (quantity → 0) on a ticker not on the watchlist: `await source.remove_ticker(ticker)`.

### 12.4 Portfolio valuation & LLM context

```python
def value_positions(positions: list[Position], cache: PriceCache) -> list[dict]:
    rows = []
    for p in positions:
        price = cache.get_price(p.ticker) or p.avg_cost   # fall back to cost if unpriced
        rows.append({
            "ticker": p.ticker, "quantity": p.quantity, "avg_cost": p.avg_cost,
            "current_price": price,
            "market_value": round(p.quantity * price, 2),
            "unrealized_pnl": round((price - p.avg_cost) * p.quantity, 2),
            "pnl_percent": round((price / p.avg_cost - 1) * 100, 2) if p.avg_cost else 0.0,
        })
    return rows
```

The chat handler builds its "watchlist with live prices" context from `cache.get_all()`, using `price` and `day_change_percent`.

### 12.5 Portfolio snapshots

A background task records `total_value = cash + Σ qty × cache.get_price(ticker)` every 30s, and also immediately after each trade (PLAN §7). It reads only the cache.

---

## 13. Testing

Unit tests live in `backend/tests/market/`. Run them with `uv run --extra dev pytest -v`. The existing 73 tests stay. Add or adjust the tests below.

### 13.1 Models & cache

```python
# tests/market/test_cache.py (additions)
from app.market import PriceCache


def test_reference_price_defaults_to_first_seen():
    c = PriceCache()
    c.update("AAPL", 190.0)
    u = c.update("AAPL", 191.9)
    assert u.reference_price == 190.0
    assert u.day_change_percent == 1.0


def test_explicit_reference_price_is_sticky():
    c = PriceCache()
    c.update("AAPL", 191.0, reference_price=189.2)
    assert c.update("AAPL", 192.0).reference_price == 189.2


def test_zero_timestamp_is_respected():
    assert PriceCache().update("X", 1.0, timestamp=0.0).timestamp == 0.0


def test_remove_bumps_version():
    c = PriceCache()
    c.update("AAPL", 1.0)
    v = c.version
    c.remove("AAPL")
    assert c.version == v + 1
```

### 13.2 Simulator

```python
# tests/market/test_simulator.py (additions)
import numpy as np

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator


def test_seeded_runs_are_deterministic():
    a = GBMSimulator(["AAPL", "MSFT"], seed=42)
    b = GBMSimulator(["AAPL", "MSFT"], seed=42)
    assert [a.step() for _ in range(100)] == [b.step() for _ in range(100)]


def test_cholesky_ok_for_large_mixed_universe():
    tickers = list(SEED_PRICES) + [f"T{i}" for i in range(40)]
    sim = GBMSimulator(tickers, seed=1)          # would raise LinAlgError if not PD
    assert len(sim.step()) == 50


def test_log_return_volatility_matches_sigma():
    sim = GBMSimulator(["V"], event_probability=0.0, seed=7)
    prices = [sim.get_price("V")] + [sim._prices["V"] for _ in range(20_000) if sim.step()]
    rets = np.diff(np.log(prices))
    realized_sigma = rets.std() / np.sqrt(GBMSimulator.DEFAULT_DT)
    assert abs(realized_sigma - 0.17) < 0.01


def test_tech_names_are_positively_correlated():
    sim = GBMSimulator(["AAPL", "MSFT"], event_probability=0.0, seed=3)
    a, m = [], []
    for _ in range(5_000):
        pa, pm = sim._prices["AAPL"], sim._prices["MSFT"]
        sim.step()
        a.append(np.log(sim._prices["AAPL"] / pa))
        m.append(np.log(sim._prices["MSFT"] / pm))
    assert 0.5 < np.corrcoef(a, m)[0, 1] < 0.7
```

### 13.3 Simulator source (async)

```python
# tests/market/test_simulator_source.py (additions)
import asyncio

from app.market import PriceCache
from app.market.simulator import SimulatorDataSource


async def test_add_ticker_normalizes_and_prices_immediately():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=0.05, seed=1)
    await src.start(["AAPL"])
    await src.add_ticker(" tsla ")
    assert "TSLA" in src.get_tickers() and cache.get_price("TSLA") == 250.0
    await asyncio.sleep(0.15)
    assert cache.version > 2
    await src.stop()
    await src.stop()  # idempotent
```

### 13.4 Massive parsing against **real SDK models** (catches C1)

```python
# tests/market/test_massive_parse.py
from massive.rest.models import TickerSnapshot

from app.market.massive_client import parse_snapshot, to_unix_seconds

RAW = {
    "ticker": "AAPL",
    "updated": 1763460000123456789,
    "day": {"o": 189.1, "h": 192.0, "l": 188.4, "c": 191.5, "v": 4.8e7},
    "prevDay": {"c": 189.2},
    "min": {"c": 191.4, "t": 1763459940000},
    "lastTrade": {"p": 191.52, "s": 100, "x": 4, "t": 1763460000123456789},
}


def test_parses_real_model():
    q = parse_snapshot(TickerSnapshot.from_dict(RAW))
    assert q.ticker == "AAPL"
    assert q.price == 191.52
    assert abs(q.timestamp - 1763460000.123) < 1e-3   # ns -> s
    assert q.reference_price == 189.2


def test_falls_back_to_minute_bar_without_last_trade():
    raw = {k: v for k, v in RAW.items() if k != "lastTrade"}
    q = parse_snapshot(TickerSnapshot.from_dict(raw))
    assert q.price == 191.4 and abs(q.timestamp - 1763459940.0) < 1e-6  # ms -> s


def test_no_price_anywhere_returns_none():
    assert parse_snapshot(TickerSnapshot.from_dict({"ticker": "ZZZZ"})) is None


def test_timestamp_units():
    assert to_unix_seconds(1763460000) == 1763460000
    assert to_unix_seconds(1763460000_000) == 1763460000
    assert to_unix_seconds(1763460000_000_000_000) == 1763460000
    assert to_unix_seconds(None) is None
```

### 13.5 Massive source behaviour (mocked fetch, real models)

**Replace the `MagicMock` snapshot helper.** The existing `_make_snapshot()` in `test_massive.py` sets `snap.last_trade.timestamp`, a field that doesn't exist on the real model. That is why C1 slipped through. With the corrected parser, `test_timestamp_conversion` and `test_malformed_snapshot_skipped` fail against `MagicMock` objects, because any attribute access returns another mock. Build every snapshot with `TickerSnapshot.from_dict(...)` as below, and mock only `_fetch_snapshots`.

```python
# tests/market/test_massive.py (additions)
import asyncio
from unittest.mock import MagicMock

from massive.rest.models import TickerSnapshot

from app.market import PriceCache
from app.market.massive_client import MassiveDataSource


def snap(t, p):
    return TickerSnapshot.from_dict({"ticker": t, "prevDay": {"c": p - 1},
                                     "lastTrade": {"p": p, "t": 1763460000000000000}})


async def test_poll_writes_real_snapshots_and_tracks_missing():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=60)
    src._client, src._tickers = MagicMock(), ["AAPL", "NOPE"]
    src._fetch_snapshots = lambda tickers: [snap("AAPL", 191.0)]
    await src._poll_once()
    assert cache.get("AAPL").reference_price == 190.0
    assert src.unpriced_tickers == {"NOPE"}


async def test_failure_backoff_and_recovery():
    src = MassiveDataSource("k", PriceCache(), poll_interval=10, max_backoff=60)
    src._client, src._tickers = MagicMock(), ["AAPL"]
    src._fetch_snapshots = MagicMock(side_effect=RuntimeError("429"))
    await src._poll_once(); await src._poll_once()
    assert src._current_delay() == 40
    src._fetch_snapshots = lambda tickers: [snap("AAPL", 1.0)]
    await src._poll_once()
    assert src._current_delay() == 10


async def test_add_ticker_triggers_early_poll():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=30, min_gap=0.05)
    src._client, src._tickers = MagicMock(), ["AAPL"]
    src._fetch_snapshots = lambda tickers: [snap(t, 10.0) for t in tickers]
    src._last_poll = asyncio.get_running_loop().time()
    src._task = asyncio.create_task(src._poll_loop())
    await src.add_ticker("msft")
    await asyncio.sleep(0.2)                    # far less than the 30s interval
    assert cache.get_price("MSFT") == 10.0
    await src.stop()
```

### 13.6 SSE generator

Test the generator directly with a fake request rather than through an HTTP client, since an infinite stream would hang the client:

```python
# tests/market/test_stream.py
import json

from app.market import PriceCache, create_stream_router
from app.market.stream import generate_price_events


class FakeRequest:
    client = None

    def __init__(self, polls_before_disconnect: int):
        self._n = polls_before_disconnect

    async def is_disconnected(self) -> bool:
        self._n -= 1
        return self._n < 0


async def test_emits_retry_then_snapshot_then_ping():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = [f async for f in generate_price_events(cache, FakeRequest(3),
                                                     interval=0.01, heartbeat=0.0)]
    assert frames[0] == "retry: 1000\n\n"
    data_line = next(l for l in frames[1].splitlines() if l.startswith("data: "))
    assert json.loads(data_line[6:])["AAPL"]["price"] == 190.0
    assert frames[2] == ": ping\n\n"           # no change since -> heartbeat


def test_router_factory_is_independent():
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert len(r1.routes) == len(r2.routes) == 1
```

### 13.7 E2E hooks

For Playwright (`test/`), run the container with `LLM_MOCK=true`, no `MASSIVE_API_KEY`, and optionally `SIMULATOR_SEED=1`. Assertions should check behaviour ("price text changes within 2s", "AAPL row exists"), not exact prices.

---

## 14. Failure modes & edge cases

| Situation | Behaviour |
|-----------|-----------|
| Empty watchlist at startup | Sources start with `[]`. The simulator steps nothing and Massive skips polls. SSE sends `retry` and then only pings until a ticker is added. |
| Invalid ticker symbol (`"$$$"`) | `normalize_ticker` raises `ValueError`, and the API returns 400 before touching the DB. |
| Well-formed but nonexistent ticker | Simulator: it gets a random price (acceptable in simulation). Massive: the API returns no data, the ticker shows up in `unpriced_tickers`, and the cache has no entry, so the watchlist shows "—" and trades are rejected with a clear error. The watchlist API may include `"price": null`. |
| Bad `MASSIVE_API_KEY` | `AuthError`/401 on every poll is logged, with backoff up to 120s. The app still serves and prices show "—". It does **not** silently fall back to the simulator: real and fake prices must never mix. |
| 429 rate limit | Counted as a failure: exponential backoff (15 → 30 → 60 → 120s), reset on the next success. |
| Network hang | SDK timeouts (5s connect / 10s read) bound each attempt. The worker thread never blocks the event loop. |
| Ticker removed mid-poll | `_poll_once` re-reads the tracked set before writing, so the removed ticker isn't written back to the cache. |
| Ticker added mid-poll | It isn't in that request's snapshot list. The wake event is still set, so another poll follows after `min_gap`. |
| Simulator exception in `step()` | Logged with traceback, and the loop continues next tick. |
| SSE client disconnect | `is_disconnected()` ends the loop within one interval. Server shutdown cancels the generator through `CancelledError`. |
| Proxy idle timeout | A `: ping` comment every 15s of silence. |
| Many SSE clients | Each client does one `get_all()` + `json.dumps` per change at 2 Hz, which is trivial for this single-user app. |
| Float drift | The simulator keeps full precision internally and only rounds at output. The cache rounds to cents. Portfolio math uses cache prices. |

---

## 15. Frontend contract (SSE payload)

The frontend relies on this event shape (one object per tracked ticker, every frame):

```ts
// frontend/src/types/market.ts
export type Direction = "up" | "down" | "flat";

export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;               // Unix seconds
  change: number;                  // vs previous tick
  change_percent: number;
  direction: Direction;            // drives flash class
  reference_price: number | null;  // prev close (Massive) / session start (sim)
  day_change: number | null;
  day_change_percent: number | null; // watchlist "change %" column
}

export type PriceFrame = Record<string, PriceUpdate>;
```

```ts
// usage sketch
const es = new EventSource("/api/stream/prices");
es.addEventListener("prices", (e) => {
  const frame: PriceFrame = JSON.parse((e as MessageEvent).data);
  for (const u of Object.values(frame)) {
    store.applyPrice(u);                       // update price, append sparkline point
    if (u.direction !== "flat") flash(u.ticker, u.direction);
  }
});
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
```

Notes for the frontend:

- A ticker that disappears from the frame has been removed. Drop it from the watchlist view, but keep it in positions if it's still held (it stays tracked in that case anyway).
- Add a sparkline point only when `timestamp` advances, so that repeated identical frames don't create duplicate points.
- With Massive, frames arrive every 2–15s rather than every 500ms. The UI must not assume a fixed cadence.
