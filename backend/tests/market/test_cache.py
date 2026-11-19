"""Tests for PriceCache."""

from app.market.cache import PriceCache


class TestPriceCache:
    """Unit tests for the PriceCache."""

    def test_update_and_get(self):
        """Test updating and getting a price."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        """Test that the first update has flat direction."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        """Test price update with upward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        """Test price update with downward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove(self):
        """Test removing a ticker from cache."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent(self):
        """Test removing a ticker that doesn't exist."""
        cache = PriceCache()
        cache.remove("AAPL")  # Should not raise

    def test_get_all(self):
        """Test getting all prices."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments(self):
        """Test that version counter increments."""
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        """Test the convenience get_price method."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_len(self):
        """Test __len__ method."""
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.00)
        assert len(cache) == 1
        cache.update("GOOGL", 175.00)
        assert len(cache) == 2

    def test_contains(self):
        """Test __contains__ method."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        assert "AAPL" in cache
        assert "GOOGL" not in cache

    def test_custom_timestamp(self):
        """Test updating with a custom timestamp."""
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts

    def test_price_rounding(self):
        """Test that prices are rounded to 2 decimal places."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12


class TestReferencePrice:
    """reference_price handling, timestamps and versioning."""

    def test_reference_price_defaults_to_first_seen(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        update = cache.update("AAPL", 191.9)
        assert update.reference_price == 190.0
        assert update.day_change_percent == 1.0

    def test_explicit_reference_price_is_sticky(self):
        cache = PriceCache()
        cache.update("AAPL", 191.0, reference_price=189.2)
        assert cache.update("AAPL", 192.0).reference_price == 189.2

    def test_explicit_reference_price_can_change(self):
        cache = PriceCache()
        cache.update("AAPL", 191.0, reference_price=189.2)
        assert cache.update("AAPL", 192.0, reference_price=191.5).reference_price == 191.5

    def test_zero_timestamp_is_respected(self):
        assert PriceCache().update("X", 1.0, timestamp=0.0).timestamp == 0.0

    def test_remove_bumps_version(self):
        cache = PriceCache()
        cache.update("AAPL", 1.0)
        version = cache.version
        cache.remove("AAPL")
        assert cache.version == version + 1

    def test_remove_unknown_does_not_bump_version(self):
        cache = PriceCache()
        version = cache.version
        cache.remove("NOPE")
        assert cache.version == version

    def test_concurrent_writers(self):
        from threading import Thread

        cache = PriceCache()

        def writer(ticker: str) -> None:
            for i in range(1000):
                cache.update(ticker, 100.0 + i)

        threads = [Thread(target=writer, args=(f"T{n}",)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert cache.version == 8000
        assert len(cache) == 8


class TestSubDollarRounding:
    """3.7: low-priced tickers keep 4 decimals instead of collapsing to 0."""

    def test_penny_price_keeps_precision(self):
        cache = PriceCache()
        update = cache.update("PENNY", 0.004217)
        assert update.price == 0.0042
        assert update.reference_price == 0.0042
        assert update.day_change_percent == 0.0

    def test_dollar_and_above_round_to_cents(self):
        cache = PriceCache()
        assert cache.update("X", 1.23456).price == 1.23
