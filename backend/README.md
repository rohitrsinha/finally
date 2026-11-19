# FinAlly Backend

FastAPI backend for the FinAlly AI Trading Workstation, managed with `uv`.

**Status:** the market data subsystem (`app/market/`) is complete, with 155 tests and 99% coverage on Python 3.12 and 3.13. The database, REST API, chat and LLM integration haven't been built yet. See `planning/PLAN.md`.

For the market data API (types, lifecycle rules, SSE frame format), see [`CLAUDE.md`](CLAUDE.md). For the full design, see `planning/MARKET_DATA_DESIGN.md`.

## Structure

- `app/` - Application code
  - `market/` - Market data subsystem
    - `models.py` - `PriceUpdate` dataclass and `round_price()` helper
    - `cache.py` - Thread-safe price cache
    - `interface.py` - `MarketDataSource` abstract interface and `normalize_ticker()`
    - `simulator.py` - GBM-based market simulator
    - `massive_client.py` - Massive/Polygon.io REST poller
    - `factory.py` - Data source factory and `MarketDataConfig` (environment variables)
    - `stream.py` - SSE streaming endpoint
    - `seed_prices.py` - Default ticker prices and parameters
- `tests/market/` - Unit and integration tests for the market data subsystem
- `market_data_demo.py` - Rich terminal dashboard showing simulated prices

## Setup and Tests

```bash
uv sync --extra dev                          # install runtime + dev dependencies

uv run --extra dev pytest                    # run all tests
uv run --extra dev pytest --cov=app          # with coverage
uv run --extra dev pytest tests/market/test_simulator.py   # one file
uv run --extra dev ruff check app/ tests/    # lint
uv run --extra dev ruff format app/ tests/   # format
```

## Demo

```bash
uv run market_data_demo.py   # live dashboard; runs 60s or until Ctrl+C
```

## Environment Variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | empty | If set, use real market data from the Massive API. Otherwise use the built-in simulator. |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between Massive polls (minimum 2). |
| `SIMULATOR_INTERVAL` | `0.5` | Simulator tick in seconds (minimum 0.05). |
| `SIMULATOR_EVENT_PROBABILITY` | `0.001` | Per-ticker, per-tick chance of a 2–5% move (clamped to 0–1). |
| `SIMULATOR_SEED` | unset | Integer seed for deterministic simulated prices. |

`OPENROUTER_API_KEY` and `LLM_MOCK` (see the root `.env.example`) will be used by the chat integration once it's built.
