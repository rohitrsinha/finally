# Market Data Backend — Summary

**Status:** Complete and tested. The design changes C1–C7 in `MARKET_DATA_DESIGN.md` and all round-2 review fixes (R1–R11, `MARKET_DATA_REVIEW.md`) are implemented and merged (PR #7).

## What Was Built

A complete market data subsystem in `backend/app/market/` (8 modules, ~500 lines) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, change, direction) |
| `interface.py` | `MarketDataSource` — abstract base class defining `start/stop/add_ticker/remove_ticker/get_tickers` |
| `cache.py` | `PriceCache` — thread-safe price store with version counter for SSE change detection |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST polling client for Polygon.io via the `massive` package |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on `MASSIVE_API_KEY` env var |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory using version-based change detection |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support

## Test Suite

**155 tests, all passing** (Python 3.12 and 3.13). 8 test modules in `backend/tests/market/`. Overall coverage: 99%.

| Module | Tests | Coverage |
|--------|-------|----------|
| test_models.py | 15 | models.py: 100% |
| test_cache.py | 22 | cache.py: 100% |
| test_interface.py | 21 | interface.py: 96% |
| test_simulator.py | 25 | simulator.py: 98% |
| test_simulator_source.py | 19 | (integration tests) |
| test_factory.py | 13 | factory.py: 100% |
| test_massive.py | 33 | massive_client.py: 99% (SDK calls mocked; snapshots are real `TickerSnapshot` objects) |
| test_stream.py | 7 | stream.py: 97% |

The round-2 review added 25 regression tests (see `MARKET_DATA_REVIEW.md` §8).

## Design Changes Applied

These are from `MARKET_DATA_DESIGN.md` §0:

- **C1** — Massive snapshot parsing uses the real SDK fields (`last_trade.sip_timestamp`, in ns), with price fallbacks and timestamp-unit normalization.
- **C2** — Daily change: `PriceUpdate.reference_price` plus `day_change` / `day_change_percent`. The baseline is the previous close (Massive) or the first price seen (simulator).
- **C3** — `create_stream_router()` builds a new router on every call. SSE frames are named `prices` events with `id:` lines, and a `: ping` heartbeat is sent after 15s without data.
- **C4** — Massive: early poll on `add_ticker()` (respecting a 12s minimum gap), exponential backoff on failures (capped at 120s), `MASSIVE_POLL_INTERVAL`, `unpriced_tickers`, and dropping tickers removed while a poll is in flight.
- **C5** — `normalize_ticker()` is shared by both sources and exported for the API layer.
- **C6** — The cache respects `timestamp=0.0`, reads `version` under the lock, and bumps the version on `remove()`.
- **C7** — The simulator uses an injected `np.random.Generator` (`seed=` / `SIMULATOR_SEED`) for deterministic runs.

## Earlier Code Review & Fixes

A comprehensive code review identified 7 issues. All were resolved:

1. **pyproject.toml build config** — added `[tool.hatch.build.targets.wheel] packages = ["app"]`
2. **Lazy imports removed** — `massive` is a core dependency; imports moved to top level
3. **SSE return type fixed** — `_generate_events` annotated as `AsyncGenerator[str, None]`
4. **Public `get_tickers()`** — added to `GBMSimulator` to avoid private attribute access
5. **Correlation constants cleaned up** — removed unused `DEFAULT_CORR`, consolidated into `CROSS_GROUP_CORR`
6. **Unused test imports removed** — `pytest`, `math`, `asyncio` cleaned from 4 test files
7. **Massive test mocks fixed** — `source._client` set in tests, patches target correct names

## Round 2 Review & Fixes

`planning/MARKET_DATA_REVIEW.md` found 11 issues (1 high, 2 medium, 6 low, 2 trivial). All are fixed:

- **High** — Massive poll loop busy-spun at 100% CPU when no tickers were tracked; it now idles until `add_ticker()`.
- `start()` twice raises `RuntimeError`; `add_ticker()`/`remove_ticker()` before `start()` are honoured by both sources.
- Simulator `dt` follows `update_interval`; `SIMULATOR_INTERVAL` / `SIMULATOR_EVENT_PROBABILITY` are clamped.
- `normalize_ticker()` raises `ValueError` for non-strings and rejects trailing punctuation.
- Prices under $1 keep 4 decimals (`round_price()`); `unpriced_tickers` ignores tickers removed mid-poll; early polls respect failure backoff; SSE no longer swallows `CancelledError`.

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY etc. from env
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None (update.day_change_percent for watchlist)
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist (also allowed before start(); applied when it runs)
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown (a source can only be started once; create a new one to restart)
await source.stop()
```
