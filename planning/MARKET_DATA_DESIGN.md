# Market Data Backend — Detailed Design

**Scope:** everything under `backend/app/market/`: the unified data-source interface, the shared price cache, the GBM simulator, the Massive (Polygon.io) REST poller, the source factory, the SSE stream endpoint, and how the rest of the backend (watchlist, portfolio, trades, chat) connects to it.

**Status:** Implemented. The changes in §0 (C1–C7) are all in `backend/app/market/`: C1 landed first on its own, and the rest followed. §11–§12 (FastAPI app wiring, and the watchlist/trade/portfolio rules) are guidance for the backend API work, which hasn't been built yet. The round-2 review fixes (R1–R11, `MARKET_DATA_REVIEW.md`) are also in, and the module listings in §2–§10 are copied verbatim from the source as of that merge (PR #7). If code and prose ever disagree, the code is authoritative.

---

## Table of Contents

0. [Changes vs. the current implementation](#0-changes-vs-the-current-implementation) (C1–C7, R1–R11)
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

### Round-2 review fixes (R1–R11)

A second review (`MARKET_DATA_REVIEW.md`) found these problems in the C1–C7 code. All are fixed:

| # | Area | Problem | Fix | Severity |
|---|------|---------|-----|----------|
| R1 | `massive_client.py` | With no tracked tickers, `_poll_loop` busy-spun at 100% CPU (a stale `_last_poll` made the timed wait return instantly). | The loop waits on the wake event until `add_ticker()` (§8.4). | High |
| R2 | both sources | A second `start()` leaked the first background task. | A second `start()` raises `RuntimeError` (§5). | Medium |
| R3 | both sources | `add_ticker()` before `start()` was lost (the simulator ignored it; Massive overwrote it in `start()`). | Pre-start adds/removes are recorded and merged by `start()` (§5, §7.2, §8.4). | Medium |
| R4 | `simulator.py`, `factory.py` | `dt` was fixed at 0.5s whatever the tick rate; `SIMULATOR_INTERVAL=0` busy-looped. | `dt = update_interval / TRADING_SECONDS_PER_YEAR`; `update_interval <= 0` raises; env values are clamped (§7.1, §9). | Low |
| R5 | `massive_client.py` | `unpriced_tickers` could list a ticker removed mid-poll. | Missing set is intersected with the re-read tracked set. | Low |
| R6 | `interface.py` | Non-string input raised `AttributeError`; `"A-"`/`"A."` were accepted. | `ValueError` for non-strings; stricter regex (§5). | Low |
| R7 | `models.py`, `cache.py` | Cent rounding turned sub-dollar prices into `0.0`. | `round_price()`: cents at $1+, 4 decimals below (§3, §4). | Low |
| R8 | `massive_client.py` | An `add_ticker()` wake-up ignored the failure backoff. | While failing, an early poll waits `max(min_gap, backoff)` (§8.4). | Low |
| R9 | `stream.py` | The generator swallowed `CancelledError`. | Cancellation propagates; `finally` still logs (§10.2). | Low |
| R10–R11 | tests | Formatter drift; redundant `event_loop_policy` fixture. | Formatted; fixture removed. | Trivial |

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

Public API:
    PriceUpdate               - Immutable price snapshot dataclass
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    MarketDataConfig          - Market data settings (from environment variables)
    normalize_ticker          - Canonical ticker normalization/validation
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router      - FastAPI router factory for SSE endpoint
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


def round_price(price: float) -> float:
    """Round a price for storage/display: cents at $1+, 4 decimals below $1.

    Plain cent rounding would turn a $0.0042 penny stock into 0.0, which would
    break valuation, fills and the daily-change baseline.
    """
    return round(price, 2) if abs(price) >= 1 else round(price, 4)


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time.

    price            latest price
    previous_price   price at the previous update (drives the up/down flash)
    timestamp        Unix seconds when the price was observed
    reference_price  baseline for "daily change": previous close (Massive)
                     or the first price seen this session (simulator)
    """

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    reference_price: float | None = None

    # --- tick-over-tick (drives the green/red flash) ---

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> Direction:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- daily (drives the watchlist "change %" column) ---

    @property
    def day_change(self) -> float | None:
        """Absolute change from the reference price, or None if unknown."""
        if self.reference_price is None:
            return None
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float | None:
        """Percentage change from the reference price, or None if unknown."""
        if not self.reference_price:  # None or 0
            return None
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
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

from .models import PriceUpdate, round_price


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every change

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        - previous_price: last cached price, or ``price`` on first sight (direction='flat').
        - reference_price: the explicit argument, else the previously cached reference,
          else ``price`` on first sight. The simulator never passes it (daily change =
          change since the session started); Massive passes the previous close.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round_price(price),
                previous_price=round_price(previous_price),
                timestamp=timestamp if timestamp is not None else time.time(),
                reference_price=round_price(reference_price) if reference_price else None,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache. Bumps the version so SSE clients see it go."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
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
```

**Why async methods?** `add_ticker` on the Massive source may wake a network poll, and future sources (WebSocket feeds) need to `await` subscriptions. Using async everywhere keeps call sites the same regardless of the source.

---

## 6. Seed data: `seed_prices.py`

No change from the current implementation. It is reproduced here so this design is complete.

```python
# backend/app/market/seed_prices.py
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
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

- `Δt = update_interval / (252 days × 6.5 h × 3600 s)`, which is ≈ 8.48e-8 at the default 0.5s tick (fraction of a trading year). Simulated time per step always equals wall-clock time per step, so the tuned volatilities hold at any `SIMULATOR_INTERVAL`.
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
from .models import round_price
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where:
        S(t)   = current price
        mu     = annualized drift (expected return)
        sigma  = annualized volatility
        dt     = time step as fraction of a trading year
        Z      = correlated standard normal random variable

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

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
        # Injected RNG: pass `seed` for deterministic price paths (tests, demos)
        self._rng = np.random.default_rng(seed)

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = self._rng.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        # Decide all shock events for this tick in one vectorized draw
        shocks = self._rng.random(n) < self._event_prob

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * self._sqrt_dt * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if shocks[i]:
                shock_magnitude = float(self._rng.uniform(0.02, 0.05))
                shock_sign = 1 if self._rng.random() < 0.5 else -1
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round_price(self._prices[ticker])

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, float(self._rng.uniform(50.0, 300.0)))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:   0.6
          - Same finance sector: 0.5
          - TSLA with anything: 0.3 (it does its own thing)
          - Cross-sector:       0.3
          - Unknown tickers:    0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache. Each step
    advances simulated time by the same `update_interval`, so the tuned
    volatilities hold whatever the cadence.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        if update_interval <= 0:
            raise ValueError(f"update_interval must be positive, got {update_interval}")
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._seed = seed
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None
        self._started = False
        self._pending: list[str] = []  # tickers added before start()

    async def start(self, tickers: list[str]) -> None:
        if self._started:
            raise RuntimeError("SimulatorDataSource.start() called twice")
        self._started = True
        # Normalize and de-duplicate while keeping order; include pre-start adds
        tickers = list(dict.fromkeys([*(normalize_ticker(t) for t in tickers), *self._pending]))
        self._pending = []
        self._sim = GBMSimulator(
            tickers=tickers,
            dt=self._interval / GBMSimulator.TRADING_SECONDS_PER_YEAR,
            event_probability=self._event_prob,
            seed=self._seed,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

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
        if self._sim is None:
            if ticker not in self._pending:
                self._pending.append(ticker)  # applied when start() runs
            return
        if ticker not in self._sim.get_tickers():
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        elif ticker in self._pending:
            self._pending.remove(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else list(self._pending)

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
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
```

### 8.4 Data source

```python
# backend/app/market/massive_client.py  (continued)
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
```

**Timing used here:** `asyncio.get_running_loop().time()` is monotonic. It's also the clock `asyncio.sleep` uses, so the deadline math stays consistent.

**Cache writes happen on the event loop.** Only the HTTP call runs in the worker thread. `_poll_once` writes to the cache after `await` returns, so writes stay on the loop thread like the simulator's.

**Rate budget example (free tier).** With the default `poll_interval=15`, steady state is 4 requests/min. If the user adds a ticker 3s after a poll, the loop wakes and then waits until `last_poll + 12s`, so the gap stays ≥ 12s (≤ 5/min). The new ticker gets a price 12s after the previous poll instead of 15s.

**Market closed.** `last_trade.price` holds the last trade (it may be after-hours). Prices won't change between polls, so `direction` stays `flat`. The UI just shows a quiet board, and nothing needs special handling.

---

## 9. Factory & configuration: `factory.py`

```python
# backend/app/market/factory.py
"""Factory for creating market data sources."""

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
    """Market data settings, normally read from environment variables."""

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
            # Floor keeps 0/negative values from turning the loop into a busy spin
            simulator_interval=max(0.05, _float_env("SIMULATOR_INTERVAL", 0.5)),
            simulator_event_probability=min(
                1.0, max(0.0, _float_env("SIMULATOR_EVENT_PROBABILITY", 0.001))
            ),
            simulator_seed=int(seed) if seed.isdigit() else None,
        )


def create_market_data_source(
    price_cache: PriceCache, config: MarketDataConfig | None = None
) -> MarketDataSource:
    """Create the appropriate market data source.

    - MASSIVE_API_KEY set and non-empty -> MassiveDataSource (real market data)
    - Otherwise -> SimulatorDataSource (GBM simulation)

    `config` defaults to MarketDataConfig.from_env(). Returns an unstarted
    source; the caller must `await source.start(tickers)`.
    """
    config = config or MarketDataConfig.from_env()

    if config.massive_api_key:
        logger.info(
            "Market data source: Massive API (real data, poll every %.1fs)",
            config.massive_poll_interval,
        )
        return MassiveDataSource(
            api_key=config.massive_api_key,
            price_cache=price_cache,
            poll_interval=config.massive_poll_interval,
        )

    logger.info("Market data source: GBM Simulator")
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
| `SIMULATOR_INTERVAL` | `0.5` | Simulator tick in seconds (floor 0.05). `dt` follows it, so volatility per wall-clock second is unchanged. |
| `SIMULATOR_EVENT_PROBABILITY` | `0.001` | Per-ticker, per-tick shock probability (clamped to 0–1). |
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
    """Create the SSE streaming router with a reference to the price cache.

    Builds a new router on every call (no module-level state), so calling it
    more than once (e.g. in tests) never registers duplicate routes.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Each frame is a named `prices` event whose data is a full snapshot of
        all tracked tickers:

            id: 1042
            event: prices
            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Clients listen with `source.addEventListener("prices", ...)`.
        """
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects.

    Sends a full price snapshot whenever the cache version changes (checked
    every `interval` seconds), and a `: ping` comment after `heartbeat`
    seconds without data so idle connections aren't closed by proxies.
    """
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

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
                yield ": ping\n\n"  # SSE comment: ignored by EventSource
                last_sent = loop.time()
            await asyncio.sleep(interval)
    finally:
        # Runs on normal disconnect and on cancellation (server shutdown);
        # CancelledError is deliberately not swallowed.
        logger.info("SSE client disconnected: %s", client_ip)
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

Unit tests live in `backend/tests/market/`. Run them with `uv run --extra dev pytest -v`. The suite now has **155 tests** (99% coverage), passing on Python 3.12 and 3.13. The snippets below are the original design-time test plan; the real tests in the repo supersede them, including the round-2 regression classes `TestMassiveLifecycleRegressions`, `TestSimulatorLifecycleRegressions`, `TestSubDollarRounding` and `TestConfigClamping`.

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
| Empty watchlist at startup | Sources start with `[]`. The simulator steps nothing, and the Massive poll loop sleeps on its wake event (no CPU, no requests) until `add_ticker()`. The same applies after the last ticker is removed. SSE sends `retry`, one empty `{}` snapshot, and then only pings until a ticker is added. |
| Invalid ticker symbol (`"$$$"`, `"A-"`, or a non-string such as `null` from LLM output) | `normalize_ticker` raises `ValueError`, and the API returns 400 (or the chat reports an error) before touching the DB. |
| `start()` called twice | Raises `RuntimeError`. Create a new source instead. |
| `add_ticker()` / `remove_ticker()` before `start()` | Recorded and applied when `start()` runs, in both sources. |
| Well-formed but nonexistent ticker | Simulator: it gets a random price (acceptable in simulation). Massive: the API returns no data, the ticker shows up in `unpriced_tickers`, and the cache has no entry, so the watchlist shows "—" and trades are rejected with a clear error. The watchlist API may include `"price": null`. |
| Bad `MASSIVE_API_KEY` | `AuthError`/401 on every poll is logged, with backoff up to 120s. The app still serves and prices show "—". It does **not** silently fall back to the simulator: real and fake prices must never mix. |
| 429 rate limit | Counted as a failure: exponential backoff (15 → 30 → 60 → 120s), reset on the next success. An `add_ticker()` wake-up during backoff still waits out the backoff. |
| Network hang | SDK timeouts (5s connect / 10s read) bound each attempt. The worker thread never blocks the event loop. |
| Ticker removed mid-poll | `_poll_once` re-reads the tracked set before writing, so the removed ticker isn't written back to the cache or reported in `unpriced_tickers`. |
| Ticker added mid-poll | It isn't in that request's snapshot list. The wake event is still set, so another poll follows after `min_gap`. |
| Simulator exception in `step()` | Logged with traceback, and the loop continues next tick. |
| SSE client disconnect | `is_disconnected()` ends the loop within one interval. Server shutdown cancels the generator through `CancelledError`, which propagates (it is not swallowed). |
| Proxy idle timeout | A `: ping` comment every 15s of silence. |
| Many SSE clients | Each client does one `get_all()` + `json.dumps` per change at 2 Hz, which is trivial for this single-user app. |
| Float drift | The simulator keeps full precision internally and only rounds at output. The cache rounds with `round_price()`: cents at $1 and above, 4 decimals below, so penny stocks never collapse to `0.0`. Portfolio math uses cache prices. |

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
