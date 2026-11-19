"""Tests for GBMSimulator."""

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator


class TestGBMSimulator:
    """Unit tests for the GBM price simulator."""

    def test_step_returns_all_tickers(self):
        """Test that step() returns prices for all tickers."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        """Test that initial prices match seed prices."""
        sim = GBMSimulator(tickers=["AAPL"])
        # Before any step, price should be the seed price
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        result = sim.step()
        assert "TSLA" in result

    def test_remove_ticker(self):
        """Test removing a ticker."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_add_duplicate_is_noop(self):
        """Test that adding a duplicate ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim._tickers) == 1

    def test_remove_nonexistent_is_noop(self):
        """Test that removing a non-existent ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")  # Should not raise

    def test_unknown_ticker_gets_random_seed_price(self):
        """Test that unknown tickers get random seed prices."""
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert price is not None
        assert 50.0 <= price <= 300.0

    def test_empty_step(self):
        """Test stepping with no tickers."""
        sim = GBMSimulator(tickers=[])
        result = sim.step()
        assert result == {}

    def test_prices_change_over_time(self):
        """After many steps, prices should have drifted from their seeds."""
        sim = GBMSimulator(tickers=["AAPL"])
        initial_price = sim.get_price("AAPL")

        for _ in range(1000):
            sim.step()

        final_price = sim.get_price("AAPL")
        # Price should have changed (extremely unlikely to be exactly the seed)
        assert final_price != initial_price

    def test_cholesky_rebuilds_on_add(self):
        """Test that Cholesky matrix is rebuilt when tickers are added."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # Only 1 ticker, no correlation matrix
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None  # Now 2 tickers, matrix exists

    def test_cholesky_none_with_one_ticker(self):
        """Test that Cholesky is None with only one ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None

    def test_get_price_returns_none_for_unknown(self):
        """Test that get_price returns None for unknown ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("UNKNOWN") is None

    def test_pairwise_correlation_tech_stocks(self):
        """Test that tech stocks have high correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "GOOGL")
        assert corr == 0.6

    def test_pairwise_correlation_finance_stocks(self):
        """Test that finance stocks have moderate correlation."""
        corr = GBMSimulator._pairwise_correlation("JPM", "V")
        assert corr == 0.5

    def test_pairwise_correlation_tsla(self):
        """Test that TSLA has lower correlation with everything."""
        corr = GBMSimulator._pairwise_correlation("TSLA", "AAPL")
        assert corr == 0.3
        corr = GBMSimulator._pairwise_correlation("TSLA", "JPM")
        assert corr == 0.3

    def test_pairwise_correlation_cross_sector(self):
        """Test cross-sector correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "JPM")
        assert corr == 0.3

    def test_default_dt_is_reasonable(self):
        """Test that default dt is a reasonable small value."""
        assert 0 < GBMSimulator.DEFAULT_DT < 0.0001

    def test_prices_rounded_to_two_decimals(self):
        """Test that prices are rounded to 2 decimal places."""
        sim = GBMSimulator(tickers=["AAPL"])
        result = sim.step()
        price_str = str(result["AAPL"])
        # Check that we have at most 2 decimal places
        if "." in price_str:
            decimal_part = price_str.split(".")[1]
            assert len(decimal_part) <= 2


class TestDeterminismAndStatistics:
    """Seeded RNG, correlation structure and volatility calibration."""

    def test_seeded_runs_are_deterministic(self):
        a = GBMSimulator(["AAPL", "MSFT", "ZZZZ"], seed=42)
        b = GBMSimulator(["AAPL", "MSFT", "ZZZZ"], seed=42)
        assert [a.step() for _ in range(200)] == [b.step() for _ in range(200)]

    def test_different_seeds_differ(self):
        a = GBMSimulator(["AAPL"], seed=1)
        b = GBMSimulator(["AAPL"], seed=2)
        assert [a.step() for _ in range(50)] != [b.step() for _ in range(50)]

    def test_cholesky_ok_for_large_mixed_universe(self):
        tickers = list(SEED_PRICES) + [f"T{i}" for i in range(40)]
        sim = GBMSimulator(tickers, seed=1)  # raises LinAlgError if not positive-definite
        assert len(sim.step()) == 50

    def test_log_return_volatility_matches_sigma(self):
        import numpy as np

        sim = GBMSimulator(["V"], event_probability=0.0, seed=7)
        prices = [sim.get_price("V")]
        for _ in range(20_000):
            sim.step()
            prices.append(sim.get_price("V"))
        realized_sigma = np.diff(np.log(prices)).std() / np.sqrt(GBMSimulator.DEFAULT_DT)
        assert abs(realized_sigma - 0.17) < 0.01

    def test_tech_names_are_positively_correlated(self):
        import numpy as np

        sim = GBMSimulator(["AAPL", "MSFT"], event_probability=0.0, seed=3)
        a, m = [], []
        for _ in range(5_000):
            pa, pm = sim.get_price("AAPL"), sim.get_price("MSFT")
            sim.step()
            a.append(np.log(sim.get_price("AAPL") / pa))
            m.append(np.log(sim.get_price("MSFT") / pm))
        assert 0.5 < np.corrcoef(a, m)[0, 1] < 0.7

    def test_shock_events_fire_with_probability_one(self):
        sim = GBMSimulator(["AAPL"], event_probability=1.0, seed=5)
        before = sim.get_price("AAPL")
        sim.step()
        move = abs(sim.get_price("AAPL") / before - 1)
        assert 0.019 < move < 0.051
