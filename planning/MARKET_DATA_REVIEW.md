# Market Data Backend — Code Review (Round 2)

**Date:** 2026-11-19
**Scope:** `backend/app/market/` (8 modules, 483 statements), `backend/tests/market/` (8 test modules), `backend/market_data_demo.py`, `backend/pyproject.toml`
**Against:** `planning/PLAN.md`, `planning/MARKET_DATA_DESIGN.md` (C1–C7), `planning/MARKET_DATA_SUMMARY.md`, `backend/CLAUDE.md`
**Previous review:** `planning/archive/MARKET_DATA_REVIEW.md` (2026-02-10). All seven issues from that review are fixed.

---

## 1. Verdict

The subsystem is in good shape and ready for the API, portfolio and chat layers to build on. The architecture is clean, C1–C7 are implemented as designed, the test suite is fast and meaningful, and an end-to-end SSE check over a real uvicorn server works.

There is **one real bug to fix before Massive mode is used**: with an empty watchlist, the Massive poll loop spins and pins a CPU core (§3.1). Everything else is low severity: edge cases and hardening worth doing while the code is fresh.

| Severity | Count | Items |
|---|---|---|
| High | 1 | 3.1 Massive busy loop when no tickers are tracked |
| Medium | 2 | 3.2 Double `start()` leaks a task · 3.3 Simulator `add_ticker()` before `start()` is silently dropped |
| Low | 6 | 3.4–3.9 |
| Trivial | 2 | 3.10–3.11 |

> **Status (2026-11-19): all findings below are resolved** on branch `fix/market-data-review-round2`. See §8 for what changed and how it was verified.

---

## 2. What Was Run

| Check | Command | Result |
|---|---|---|
| Dependency sync | `uv sync --extra dev` | OK |
| Unit tests | `uv run --extra dev pytest --cov=app` | **130 passed, 0 failed** in ~4s |
| Coverage | (same) | **98%** overall (483 stmts, 9 missed) |
| Warnings | `pytest -W default` | None |
| Lint | `ruff check app/ tests/ market_data_demo.py` | All checks passed |
| Format | `ruff format --check app/ tests/` | **3 test files would be reformatted** (see 3.10) |
| Live SSE | Minimal FastAPI app + `uvicorn`, `curl -N /api/stream/prices` for 2s | 200 OK, correct headers, `retry:` then 4 `prices` frames with all 10 tickers |
| Demo | `uv run market_data_demo.py` (4s smoke run) | Runs without error |
| Edge-case probes | Ad-hoc scripts (scratchpad, not committed) | Confirmed 3.1, 3.2, 3.3, 3.5, 3.6, 3.7 |

Coverage by module:

| Module | Cover | Missed lines |
|---|---|---|
| models.py | 100% | |
| cache.py | 100% | |
| factory.py | 100% | |
| seed_prices.py | 100% | |
| interface.py | 96% | 77 (`name` property) |
| massive_client.py | 98% | 50, 200, 256 (bool guard, early-wake `return`, real SDK call) |
| simulator.py | 98% | 155 (duplicate guard), 281–282 (exception log) |
| stream.py | 95% | 43 (route body), 91 (`CancelledError`) |

Note: the local venv runs **Python 3.13**; the Dockerfile in PLAN §11 targets **3.12**. `requires-python = ">=3.12"` and ruff `target-version = "py312"` keep this compatible, but the suite should also be run on 3.12 once the Dockerfile exists.

---

## 3. Findings

### 3.1 Massive poll loop busy-spins with no tracked tickers — **High**

`massive_client.py:205-208`, `:194-198`

`_poll_once()` returns early when `self._tickers` is empty **without updating `self._last_poll`**. `_wait_for_next_poll()` then computes `due = self._last_poll + delay`, which is already in the past, so `wait_for(..., timeout=0)` returns at once and the loop repeats.

Measured with `start([])`: **`_poll_once` called 85,566 times in 1 second, 1.00s of CPU in 1.00s of wall time.**

It happens in two cases:
- Startup with an empty watchlist (an explicitly supported case, design §14).
- After the user removes every ticker: once `poll_interval` has passed since the last real poll, the loop starts spinning.

No API calls are made, so there's no rate-limit damage, but the event loop is starved and the container burns a core. It doesn't affect the simulator, and the tests miss it because none of them run `_poll_loop` with an empty list.

**Fix (pick one):**
- In `_poll_once`, set `self._last_poll = loop.time()` before the early return; or
- In `_poll_loop`, when `self._tickers` is empty, `await self._wake.wait()` (block until `add_ticker` sets it) instead of using the timed wait. This is cleaner: zero work while idle.

Add a test: `start([])`, sleep ~0.2s, assert `_poll_once` ran a small, bounded number of times.

### 3.2 Calling `start()` twice leaks the first background task — Medium

`simulator.py:227-241`, `massive_client.py:132-144`

The interface says calling `start()` twice is "undefined behavior". In practice the second call overwrites `self._task`, so `stop()` cancels only the new task. Probe: after `start(); start(); stop()` the first simulator task **is still running** and keeps writing to the cache with a stale `GBMSimulator`.

This is easy to hit by accident, e.g. a lifespan bug or a test fixture that reuses a source. **Fix:** guard at the top of `start()` (`if self._task is not None: raise RuntimeError("already started")`, or make it a no-op) and add a test.

### 3.3 Simulator drops `add_ticker()` calls made before `start()` — Medium

`simulator.py:253-261`

`SimulatorDataSource.add_ticker()` does nothing when `self._sim is None`. `MassiveDataSource.add_ticker()` appends to its list and the ticker is polled after `start()`. Probe: `add_ticker("PYPL"); start(["AAPL"])` gives `['AAPL']` for the simulator but `['PYPL']` tracked for Massive. The two implementations of one interface disagree.

With the design §11 lifespan this ordering shouldn't occur, but a watchlist request racing startup could trigger it. **Fix:** keep a pending set in the simulator and merge it in `start()`, or raise in both sources if they're used before `start()`. Either way, make the two match and document the rule in `interface.py`.

### 3.4 `SIMULATOR_INTERVAL` changes cadence but not `dt` — Low

`simulator.py:230-234`, `factory.py:44`

`SimulatorDataSource` never passes `dt` to `GBMSimulator`, so `dt` is always `0.5 / TRADING_SECONDS_PER_YEAR`, whatever `update_interval` is. With `SIMULATOR_INTERVAL=0.1`, prices move 5× faster in wall-clock time than the tuned volatilities intend; with `2.0` they move 4× slower.

Also, the interval isn't bounded: `SIMULATOR_INTERVAL=0` (or negative) makes `_run_loop` a tight `sleep(0)` loop. `MASSIVE_POLL_INTERVAL` is clamped (`max(2.0, …)`), but the simulator interval isn't.

**Fix:** pass `dt=update_interval / GBMSimulator.TRADING_SECONDS_PER_YEAR`, and clamp the interval (e.g. `max(0.05, …)`). Clamp `SIMULATOR_EVENT_PROBABILITY` to `[0, 1]` too.

### 3.5 `unpriced_tickers` can report a ticker that was removed during the poll — Low

`massive_client.py:249`

`self._missing = set(tickers) - seen` uses the pre-fetch snapshot `tickers`, not the re-read `tracked` set. Probe: remove `MSFT` while a poll is in flight, and `unpriced_tickers` returns `{'MSFT'}` with a "no data for: MSFT" warning, even though MSFT is no longer tracked. It clears on the next poll. **Fix:** `self._missing = (set(tickers) & tracked) - seen`.

### 3.6 `normalize_ticker()` raises `AttributeError` on non-strings — Low

`interface.py:17`

`normalize_ticker(None)` and `normalize_ticker(123)` raise `AttributeError` instead of the documented `ValueError`. The design (§12.1) tells API routes to catch `ValueError` and return 400. LLM structured output can easily produce a `null` or numeric ticker, and that would become a 500. **Fix:** `if not isinstance(raw, str): raise ValueError(...)`.

The regex also accepts trailing punctuation (`"A-"`, `"A."`). That's harmless but easy to tighten: `^[A-Z][A-Z0-9]*([.\-][A-Z0-9]+)?$` with a length check.

### 3.7 Cache rounding to cents erases sub-dollar prices — Low

`cache.py:45-48`

All prices are stored as `round(price, 2)`. A $0.0042 price is stored as `0.0`, `reference_price` becomes `0.0`, and `day_change_percent` becomes `None`. The default universe is large caps, so this doesn't matter in practice, but a user could add a penny stock in Massive mode. A price of `0.0` would also make portfolio valuation and trade fills wrong. **Fix (optional):** round to 4 decimals below $1, or keep full precision in the cache and round only at the presentation layer.

### 3.8 Adding a ticker bypasses failure backoff — Low

`massive_client.py:194-203`

An `add_ticker()` wake-up waits only for `min_gap` (12s) and ignores `_current_delay()`. With a bad API key (401 on every poll), each watchlist add causes another failed request after 12s instead of waiting out the 30/60/120s backoff. It's still within the 5 req/min limit, so the impact is small. **Fix:** `earliest = self._last_poll + max(self._min_gap, self._current_delay() if self._failures else 0)`.

### 3.9 SSE generator swallows `CancelledError` — Low

`stream.py:90-91`

Catching `asyncio.CancelledError` and returning normally hides cancellation from the caller. Under anyio (Starlette) this happens to work, because cancel scopes re-deliver cancellation, but it's against asyncio guidance and can hide shutdown issues. **Fix:** drop the `except` clause and keep the `finally` for logging.

### 3.10 Formatter drift in tests — Trivial

`ruff format --check` wants to reformat `tests/market/test_models.py`, `test_simulator.py` and `test_simulator_source.py`, mostly long `PriceUpdate(...)` lines. Run `uv run --extra dev ruff format tests/`.

### 3.11 `conftest.py` overrides `event_loop_policy` with the default — Trivial

The fixture returns `asyncio.DefaultEventLoopPolicy()`, which is already the default. Event-loop policies are deprecated in Python 3.14. The fixture can be deleted.

---

## 4. Conformance to the Design (C1–C7)

| # | Requirement | Status | Notes |
|---|---|---|---|
| C1 | Real SDK fields, fallbacks, timestamp units | ✅ | `parse_snapshot` uses `last_trade.price/sip_timestamp`, then `min`, `day`, `prev_day`. Tests use real `TickerSnapshot` objects. `get_snapshot_all(market_type, tickers)` matches the installed SDK signature. |
| C2 | `reference_price`, `day_change(_percent)` | ✅ | Previous close for Massive, first price seen for the simulator. The reference is kept across updates when omitted. |
| C3 | Router per call, named `prices` events, `id:`, heartbeat | ✅ | Verified over live HTTP. |
| C4 | Early poll, min gap, backoff, interval env, `unpriced_tickers`, mid-flight removal | ✅ | Tests cover these, with the edge cases in 3.1, 3.5 and 3.8. |
| C5 | Shared `normalize_ticker` | ✅ | Used by both sources and exported (see 3.6). |
| C6 | `timestamp=0.0` respected, locked `version`, version bump on remove | ✅ | |
| C7 | Injected RNG / `SIMULATOR_SEED` | ✅ | `default_rng(seed)`. Unknown-ticker seed prices are also deterministic. |

The PLAN §6 requirements are met: GBM at ~500ms, correlated moves (Cholesky over sector groups), 2–5% shock events, realistic seed prices, Massive REST polling for the union of tickers, a shared price cache, and SSE carrying ticker/price/previous/timestamp/direction.

---

## 5. Strengths

- **Clean boundaries.** Strategy pattern, with `PriceCache` as the only shared state. Downstream code never touches a source for prices.
- **Correct GBM.** It uses the log-normal step `exp((μ − σ²/2)dt + σ√dt·Z)` with full internal precision, and shocks are decided in one vectorized draw.
- **Thread safety.** The Massive SDK call runs in `asyncio.to_thread`, and all cache writes happen back on the event loop. Simulator writes for one tick happen without an `await`, so an SSE frame never sees a half-updated tick.
- **Defensive Massive client.** It handles missing or invalid fields, mixed timestamp units, exponential backoff, rate-limit-aware early polls, and doesn't resurrect tickers removed mid-flight.
- **Fast, deterministic tests** (130 in ~4s) that use real SDK models instead of `MagicMock` snapshots, which is how C1 was caught.
- **Good documentation.** `backend/CLAUDE.md`, the design doc's §11–§15 integration notes and the frontend TypeScript contract give the next agents what they need.

---

## 6. Notes for Downstream Agents

These aren't defects in this module, but the next agents need to know them:

1. **Use one uvicorn worker.** The cache and source are per-process (design §11).
2. **Watchlist removal must not stop tracking held tickers** (design §12.2). Otherwise positions lose their price.
3. **Catch `ValueError` from `normalize_ticker`** in API routes and the LLM action executor, and return 400 or a chat error. Until 3.6 is fixed, guard against non-string tickers from the LLM before calling it.
4. **SSE frames are full snapshots.** A ticker missing from a frame has been removed. Listen with `addEventListener("prices", …)`, not `onmessage`.
5. **`id:` is the cache version**, which grows by about 20/s with the simulator (10 tickers × 2 Hz). It's an opaque counter; don't treat it as a frame index.
6. **Fill prices come from `cache.get_price()`**, which can be `None` for a just-added Massive ticker. Reject the trade with a clear message (design §12.3).

---

## 7. Recommended Actions

**Must fix (before enabling Massive mode):**
1. Fix the empty-ticker busy loop (3.1) and add a regression test.

**Should fix (small, low-risk):**
2. Guard against double `start()` (3.2).
3. Make `add_ticker()` before `start()` behave the same in both sources (3.3).
4. Tie simulator `dt` to the interval and clamp the env values (3.4).
5. Make `normalize_ticker` raise `ValueError` for non-strings (3.6).
6. Intersect `_missing` with the tracked set (3.5).

**Nice to have:**
7. Respect backoff on early wake (3.8), re-raise `CancelledError` in SSE (3.9), and review cent rounding for low-priced tickers (3.7).
8. Run `ruff format tests/` and delete the `event_loop_policy` fixture (3.10, 3.11).
9. Run the suite under Python 3.12 to match the Docker image.

---

## 8. Resolution

Every finding was fixed, and each fix has a regression test. **155 tests pass** (130 → 155), coverage is **99%**, and `ruff check` and `ruff format --check` are clean. The suite passes on both **Python 3.13 and 3.12**. 15 of the new tests were run against the old source and confirmed to fail there; the remaining 2 cover new forward behaviour.

| # | Fix | Where | Test |
|---|---|---|---|
| 3.1 | `_poll_loop` waits on the wake event when no tickers are tracked. Re-probed: 1 call / 0.00s CPU in 1s, down from 85,566 / 1.00s | `massive_client.py` | `test_empty_watchlist_does_not_busy_loop`, `test_removing_all_tickers_idles`, `test_idle_loop_wakes_on_add_ticker` |
| 3.2 | A second `start()` raises `RuntimeError` in both sources (`_started` flag) | `simulator.py`, `massive_client.py`, `interface.py` docstring | `test_double_start_raises` ×2 |
| 3.3 | Pre-start `add_ticker`/`remove_ticker` are recorded and merged by `start()`, the same way in both sources. Massive drops the stale wake because the first poll already covers them | `simulator.py`, `massive_client.py` | `test_add_and_remove_before_start`, `test_add_ticker_before_start_is_kept` |
| 3.4 | Simulator `dt = update_interval / TRADING_SECONDS_PER_YEAR`. `update_interval <= 0` raises. `SIMULATOR_INTERVAL` has a 0.05s floor and `SIMULATOR_EVENT_PROBABILITY` is clamped to [0, 1] | `simulator.py`, `factory.py` | `test_dt_follows_update_interval`, `test_non_positive_interval_rejected`, `TestConfigClamping` |
| 3.5 | `_missing = (set(tickers) & tracked) - seen` | `massive_client.py` | `test_unpriced_excludes_ticker_removed_mid_poll` |
| 3.6 | Non-string input raises `ValueError`. Regex tightened to one optional `.`/`-` class suffix, max 10 chars | `interface.py` | `test_rejects_non_string_with_value_error`, extended `test_rejects_invalid_tickers` |
| 3.7 | New `round_price()`: cents at $1 and above, 4 decimals below. Used by the cache and the simulator | `models.py`, `cache.py`, `simulator.py` | `TestSubDollarRounding` |
| 3.8 | While failing, an early wake waits `max(min_gap, backoff)` | `massive_client.py` | `test_early_wake_respects_backoff` |
| 3.9 | `except CancelledError: pass` removed; the `finally` logging is kept | `stream.py` | `test_cancellation_propagates` |
| 3.10 | `ruff format` applied to the tests | `tests/` | `ruff format --check` |
| 3.11 | The redundant `event_loop_policy` fixture is removed | `tests/conftest.py` | n/a |

The live SSE check over uvicorn was re-run after the changes: `retry:` frame followed by `prices` frames with all 10 tickers.

**Behaviour changes for downstream code:**
- `start()` may only be called once per source instance.
- `add_ticker()` before `start()` is now honoured.
- `normalize_ticker(None)` now raises `ValueError` instead of `AttributeError`.
- Prices under $1 now carry 4 decimals.
