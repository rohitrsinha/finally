# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Use these imports:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource, MarketDataConfig,
    create_market_data_source, create_stream_router, normalize_ticker,
)
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, `reference_price` (previous close for Massive, first price seen this session for the simulator), plus properties `change`, `change_percent`, `direction` ("up"/"down"/"flat") for tick-over-tick moves, `day_change` / `day_change_percent` for the daily change, and `to_dict()` for JSON serialization.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, reference_price=None) -> PriceUpdate` (reference price is kept from the previous update when omitted; prices are rounded to cents, or 4 decimals below $1)
  - `get(ticker) -> PriceUpdate | None`
  - `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`
  - `remove(ticker)` (bumps the version so SSE clients see the removal)
  - `version` property — monotonic counter, increments on every change (for SSE change detection)

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)` -> `add_ticker()` / `remove_ticker()` -> `stop()`. `start()` may be called only once per instance (a second call raises `RuntimeError`); ticker changes made before `start()` are applied when it runs.

- **`normalize_ticker(raw)`** — Upper-cases and strips a symbol; raises `ValueError` if invalid, including non-string input such as a `null` ticker from the LLM. Use it in API routes before touching the DB. Both sources apply it too.

- **`create_market_data_source(cache, config=None)`** — Factory. Returns `MassiveDataSource` if `MASSIVE_API_KEY` is set, otherwise `SimulatorDataSource`. `config` defaults to `MarketDataConfig.from_env()`, which reads `MASSIVE_POLL_INTERVAL`, `SIMULATOR_INTERVAL`, `SIMULATOR_EVENT_PROBABILITY` and `SIMULATOR_SEED`.

- **Massive specifics** — Polls every 15s by default. `add_ticker()` triggers an early poll, never sooner than 12s after the previous one (free tier: 5 req/min). Repeated failures back off exponentially up to 120s. `unpriced_tickers` lists tracked tickers the API returned no data for.

### SSE Streaming

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)  # Returns a new FastAPI APIRouter each call
# Endpoint: GET /api/stream/prices (text/event-stream)
```

Each frame is a named `prices` event (`id: <version>`, `event: prices`, `data: {ticker: PriceUpdate.to_dict(), ...}`) holding a full snapshot of all tracked tickers. Frontend: `es.addEventListener("prices", ...)`, not `onmessage`. When nothing changes for 15s the server sends a `: ping` comment.

See `planning/MARKET_DATA_DESIGN.md` for the full design, including how watchlist, trade and portfolio routes should use the market data layer.

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
