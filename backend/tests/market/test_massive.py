"""Tests for MassiveDataSource (mocked)."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, parse_snapshot, to_unix_seconds


def _make_snapshot(ticker: str, price: float, timestamp_ns: int) -> TickerSnapshot:
    """Build a real massive TickerSnapshot from raw API JSON.

    Uses the SDK's own parser so tests exercise the real field names
    (last_trade.sip_timestamp, in nanoseconds) rather than a permissive mock.
    """
    return TickerSnapshot.from_dict(
        {
            "ticker": ticker,
            "lastTrade": {"p": price, "s": 100, "x": 4, "t": timestamp_ns},
        }
    )


TS_NS = 1707580800_000_000_000  # 2024-02-10T16:00:00Z in nanoseconds


@pytest.mark.asyncio
class TestMassiveDataSource:
    """Unit tests for MassiveDataSource with mocked API."""

    async def test_poll_updates_cache(self):
        """Test that polling updates the cache."""
        cache = PriceCache()
        source = MassiveDataSource(
            api_key="test-key",
            price_cache=cache,
            poll_interval=60.0,  # Long interval so the loop doesn't auto-poll
        )
        source._tickers = ["AAPL", "GOOGL"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        mock_snapshots = [
            _make_snapshot("AAPL", 190.50, TS_NS),
            _make_snapshot("GOOGL", 175.25, TS_NS),
        ]

        with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_malformed_snapshot_skipped(self):
        """Test that malformed snapshots are skipped gracefully."""
        cache = PriceCache()
        source = MassiveDataSource(
            api_key="test-key",
            price_cache=cache,
            poll_interval=60.0,
        )
        source._tickers = ["AAPL", "BAD"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        good_snap = _make_snapshot("AAPL", 190.50, TS_NS)
        bad_snap = TickerSnapshot.from_dict({"ticker": "BAD"})  # no price anywhere

        with patch.object(source, "_fetch_snapshots", return_value=[good_snap, bad_snap]):
            await source._poll_once()

        # Good ticker processed, bad one skipped
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        """Test that API errors don't crash the poller."""
        cache = PriceCache()
        source = MassiveDataSource(
            api_key="test-key",
            price_cache=cache,
            poll_interval=60.0,
        )
        source._tickers = ["AAPL"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            await source._poll_once()  # Should not raise

        assert cache.get_price("AAPL") is None  # No update happened

    async def test_timestamp_conversion(self):
        """Test that nanosecond trade timestamps are converted to seconds."""
        cache = PriceCache()
        source = MassiveDataSource(
            api_key="test-key",
            price_cache=cache,
            poll_interval=60.0,
        )
        source._tickers = ["AAPL"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        mock_snapshots = [_make_snapshot("AAPL", 190.50, TS_NS)]

        with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update is not None
        assert update.timestamp == 1707580800.0  # Converted to seconds

    async def test_add_ticker(self):
        """Test adding a ticker."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("AAPL")
        assert "AAPL" in source.get_tickers()

    async def test_add_ticker_uppercase_normalization(self):
        """Test that tickers are normalized to uppercase."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("aapl")
        assert "AAPL" in source.get_tickers()

    async def test_add_ticker_strips_whitespace(self):
        """Test that ticker whitespace is stripped."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("  AAPL  ")
        assert "AAPL" in source.get_tickers()

    async def test_remove_ticker(self):
        """Test removing a ticker."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = ["AAPL", "GOOGL"]
        cache.update("AAPL", 190.00)

        await source.remove_ticker("AAPL")
        assert "AAPL" not in source.get_tickers()
        assert cache.get("AAPL") is None

    async def test_get_tickers(self):
        """Test getting the list of active tickers."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = ["AAPL", "GOOGL"]

        tickers = source.get_tickers()
        assert tickers == ["AAPL", "GOOGL"]

    async def test_empty_tickers_skips_poll(self):
        """Test that polling is skipped when there are no tickers."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = []

        # Should not call _fetch_snapshots
        with patch.object(source, "_fetch_snapshots") as mock_fetch:
            await source._poll_once()
            mock_fetch.assert_not_called()

    async def test_stop_is_idempotent(self):
        """Test that stop() can be called multiple times."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.stop()
        await source.stop()  # Should not raise

    async def test_stop_cancels_task(self):
        """Test that stop() cancels the polling task."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=10.0)

        # Mock the client and start
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[]):
                await source.start(["AAPL"])

        # Verify task is running
        assert source._task is not None
        assert not source._task.done()

        # Stop and verify task is cancelled
        await source.stop()
        assert source._task is None

    async def test_start_immediate_poll(self):
        """Test that start() does an immediate poll before starting the loop."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)

        mock_snapshots = [_make_snapshot("AAPL", 190.50, TS_NS)]

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
                await source.start(["AAPL"])

        # Cache should have data immediately from the first poll
        assert cache.get_price("AAPL") == 190.50

        await source.stop()


class TestParseSnapshot:
    """parse_snapshot() against real SDK model objects."""

    RAW = {
        "ticker": "AAPL",
        "updated": 1763460000_123456789,
        "day": {"o": 189.1, "h": 192.0, "l": 188.4, "c": 191.5, "v": 4.8e7},
        "prevDay": {"c": 189.2},
        "min": {"c": 191.4, "t": 1763459940_000},
        "lastTrade": {"p": 191.52, "s": 100, "x": 4, "t": 1763460000_123456789},
    }

    def test_uses_last_trade_price_and_sip_timestamp(self):
        q = parse_snapshot(TickerSnapshot.from_dict(self.RAW))
        assert q.ticker == "AAPL"
        assert q.price == 191.52
        assert abs(q.timestamp - 1763460000.123456789) < 1e-6
        assert q.reference_price == 189.2  # previous close

    def test_falls_back_to_minute_bar(self):
        raw = {k: v for k, v in self.RAW.items() if k != "lastTrade"}
        q = parse_snapshot(TickerSnapshot.from_dict(raw))
        assert q.price == 191.4
        assert q.timestamp == 1763459940.0  # milliseconds -> seconds

    def test_falls_back_to_day_then_prev_close(self):
        raw = {
            "ticker": "AAPL",
            "updated": 1763460000_000_000_000,
            "day": {"c": 191.5},
            "prevDay": {"c": 189.2},
        }
        q = parse_snapshot(TickerSnapshot.from_dict(raw))
        assert q.price == 191.5
        assert q.timestamp == 1763460000.0  # from `updated`

        raw = {"ticker": "AAPL", "day": {"c": 0}, "prevDay": {"c": 189.2}}  # pre-market zeros
        q = parse_snapshot(TickerSnapshot.from_dict(raw))
        assert q.price == 189.2

    def test_no_usable_price_returns_none(self):
        assert parse_snapshot(TickerSnapshot.from_dict({"ticker": "ZZZZ"})) is None
        assert parse_snapshot(TickerSnapshot.from_dict({"lastTrade": {"p": 1.0}})) is None

    def test_to_unix_seconds_units(self):
        assert to_unix_seconds(1763460000) == 1763460000
        assert to_unix_seconds(1763460000_000) == 1763460000
        assert to_unix_seconds(1763460000_000_000) == 1763460000
        assert to_unix_seconds(1763460000_000_000_000) == 1763460000
        assert to_unix_seconds(None) is None
        assert to_unix_seconds(0) is None

    def test_missing_prev_close_gives_no_reference(self):
        raw = {"ticker": "AAPL", "lastTrade": {"p": 191.0, "t": 1763460000_000_000_000}}
        assert parse_snapshot(TickerSnapshot.from_dict(raw)).reference_price is None


def _snap_with_prev_close(ticker: str, price: float) -> TickerSnapshot:
    return TickerSnapshot.from_dict(
        {
            "ticker": ticker,
            "prevDay": {"c": price - 1},
            "lastTrade": {"p": price, "t": TS_NS},
        }
    )


@pytest.mark.asyncio
class TestMassivePolling:
    """Reference price, missing tickers, backoff and early wake-up."""

    async def test_poll_sets_reference_price_and_tracks_missing(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60)
        source._client = MagicMock()
        source._tickers = ["AAPL", "NOPE"]
        source._fetch_snapshots = lambda tickers: [_snap_with_prev_close("AAPL", 191.0)]

        await source._poll_once()

        update = cache.get("AAPL")
        assert update.reference_price == 190.0
        assert update.day_change_percent == round(1 / 190 * 100, 4)
        assert source.unpriced_tickers == {"NOPE"}

    async def test_fetch_receives_snapshot_of_tickers(self):
        source = MassiveDataSource(api_key="k", price_cache=PriceCache(), poll_interval=60)
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        fetch = MagicMock(return_value=[])
        source._fetch_snapshots = fetch

        await source._poll_once()

        fetch.assert_called_once_with(["AAPL"])

    async def test_ticker_removed_mid_poll_is_not_written(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60)
        source._client = MagicMock()
        source._tickers = ["AAPL", "MSFT"]

        def fetch(tickers):
            source._tickers = ["AAPL"]  # MSFT removed while the request is in flight
            return [_snap_with_prev_close(t, 100.0) for t in tickers]

        source._fetch_snapshots = fetch
        await source._poll_once()

        assert "AAPL" in cache
        assert "MSFT" not in cache

    async def test_failure_backoff_and_recovery(self):
        source = MassiveDataSource(
            api_key="k", price_cache=PriceCache(), poll_interval=10, max_backoff=60
        )
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        source._fetch_snapshots = MagicMock(side_effect=RuntimeError("429"))

        await source._poll_once()
        assert source._current_delay() == 20
        await source._poll_once()
        assert source._current_delay() == 40
        await source._poll_once()
        await source._poll_once()
        assert source._current_delay() == 60  # capped

        source._fetch_snapshots = lambda tickers: [_snap_with_prev_close("AAPL", 1.0)]
        await source._poll_once()
        assert source._current_delay() == 10

    async def test_add_ticker_triggers_early_poll(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=30, min_gap=0.05)
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        source._fetch_snapshots = lambda tickers: [_snap_with_prev_close(t, 10.0) for t in tickers]
        source._last_poll = asyncio.get_running_loop().time()
        source._task = asyncio.create_task(source._poll_loop())

        await source.add_ticker("msft")
        await asyncio.sleep(0.3)  # far less than the 30s interval

        assert cache.get_price("MSFT") == 10.0
        await source.stop()

    async def test_early_poll_respects_min_gap(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=30, min_gap=0.5)
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        source._fetch_snapshots = lambda tickers: [_snap_with_prev_close(t, 10.0) for t in tickers]
        source._last_poll = asyncio.get_running_loop().time()
        source._task = asyncio.create_task(source._poll_loop())

        await source.add_ticker("MSFT")
        await asyncio.sleep(0.2)
        assert "MSFT" not in cache  # still inside the minimum gap
        await asyncio.sleep(0.5)
        assert cache.get_price("MSFT") == 10.0
        await source.stop()

    async def test_invalid_ticker_rejected(self):
        source = MassiveDataSource(api_key="k", price_cache=PriceCache())
        with pytest.raises(ValueError):
            await source.add_ticker("$$$")


@pytest.mark.asyncio
class TestMassiveLifecycleRegressions:
    """Regressions from planning/MARKET_DATA_REVIEW.md (round 2)."""

    async def test_empty_watchlist_does_not_busy_loop(self):
        """3.1: with no tickers the poll loop must idle, not spin."""
        source = MassiveDataSource(api_key="k", price_cache=PriceCache(), poll_interval=15.0)
        calls = 0
        original = source._poll_once

        async def counting_poll():
            nonlocal calls
            calls += 1
            await original()

        source._poll_once = counting_poll
        with patch("app.market.massive_client.RESTClient"):
            await source.start([])
        await asyncio.sleep(0.2)
        assert calls <= 2  # previously ~17,000 in 0.2s
        await source.stop()

    async def test_idle_loop_wakes_on_add_ticker(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=30, min_gap=0.05)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(
                source,
                "_fetch_snapshots",
                side_effect=lambda tickers: [_snap_with_prev_close(t, 10.0) for t in tickers],
            ):
                await source.start([])
                await source.add_ticker("AAPL")
                await asyncio.sleep(0.3)
                assert cache.get_price("AAPL") == 10.0
                await source.stop()

    async def test_removing_all_tickers_idles(self):
        """Once the last poll is older than poll_interval, an empty list used to spin."""
        source = MassiveDataSource(api_key="k", price_cache=PriceCache(), poll_interval=0.05)
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        source._fetch_snapshots = MagicMock(return_value=[])
        calls = 0
        original = source._poll_once

        async def counting_poll():
            nonlocal calls
            calls += 1
            await original()

        source._poll_once = counting_poll
        source._task = asyncio.create_task(source._poll_loop())
        await source.remove_ticker("AAPL")
        await asyncio.sleep(0.3)  # six poll intervals
        assert calls <= 2
        await source.stop()

    async def test_double_start_raises(self):
        """3.2: a second start() must not leak the first poll task."""
        source = MassiveDataSource(api_key="k", price_cache=PriceCache(), poll_interval=60)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[]):
                await source.start(["AAPL"])
                with pytest.raises(RuntimeError):
                    await source.start(["AAPL"])
        await source.stop()

    async def test_add_ticker_before_start_is_kept(self):
        """3.3: pre-start adds are merged into the start() list."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60)
        await source.add_ticker("pypl")
        fetch = MagicMock(
            side_effect=lambda tickers: [_snap_with_prev_close(t, 10.0) for t in tickers]
        )
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", fetch):
                await source.start(["AAPL", "PYPL"])
                assert source.get_tickers() == ["AAPL", "PYPL"]
                assert cache.get_price("PYPL") == 10.0
                assert not source._wake.is_set()  # first poll covered the early add
                await source.stop()

    async def test_unpriced_excludes_ticker_removed_mid_poll(self):
        """3.5"""
        source = MassiveDataSource(api_key="k", price_cache=PriceCache())
        source._client = MagicMock()
        source._tickers = ["AAPL", "MSFT"]

        def fetch(tickers):
            source._tickers = ["AAPL"]  # MSFT removed while the request is in flight
            return [_snap_with_prev_close("AAPL", 10.0)]

        source._fetch_snapshots = fetch
        await source._poll_once()
        assert source.unpriced_tickers == set()

    async def test_early_wake_respects_backoff(self):
        """3.8: an add_ticker() wake-up waits out the failure backoff, not just min_gap."""
        source = MassiveDataSource(
            api_key="k", price_cache=PriceCache(), poll_interval=0.2, min_gap=0.01
        )
        source._client = MagicMock()
        source._tickers = ["AAPL"]
        source._fetch_snapshots = MagicMock(side_effect=RuntimeError("401"))
        await source._poll_once()  # 1 failure -> backoff 0.4s
        source._task = asyncio.create_task(source._poll_loop())
        await source.add_ticker("MSFT")
        await asyncio.sleep(0.2)
        assert source._fetch_snapshots.call_count == 1  # min_gap alone would allow a retry
        await source.stop()
