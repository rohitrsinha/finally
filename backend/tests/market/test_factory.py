"""Tests for market data source factory."""

import os
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import MarketDataConfig, create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestFactory:
    """Tests for create_market_data_source factory."""

    def test_creates_simulator_when_no_api_key(self):
        """Test that simulator is created when MASSIVE_API_KEY is not set."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_empty(self):
        """Test that simulator is created when MASSIVE_API_KEY is empty."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_whitespace(self):
        """Test that simulator is created when MASSIVE_API_KEY is whitespace."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_massive_when_api_key_set(self):
        """Test that Massive client is created when MASSIVE_API_KEY is set."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)

    def test_massive_receives_api_key(self):
        """Test that Massive client receives the API key."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._api_key == "test-key-123"

    def test_simulator_receives_cache(self):
        """Test that simulator receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)
        assert source._cache is cache

    def test_massive_receives_cache(self):
        """Test that Massive client receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._cache is cache


class TestMarketDataConfig:
    """Environment-driven configuration."""

    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            config = MarketDataConfig.from_env()
        assert config == MarketDataConfig()

    def test_reads_all_variables(self):
        env = {
            "MASSIVE_API_KEY": " key ",
            "MASSIVE_POLL_INTERVAL": "5",
            "SIMULATOR_INTERVAL": "0.25",
            "SIMULATOR_EVENT_PROBABILITY": "0.01",
            "SIMULATOR_SEED": "42",
        }
        with patch.dict(os.environ, env, clear=True):
            config = MarketDataConfig.from_env()
        assert config.massive_api_key == "key"
        assert config.massive_poll_interval == 5.0
        assert config.simulator_interval == 0.25
        assert config.simulator_event_probability == 0.01
        assert config.simulator_seed == 42

    def test_poll_interval_floor_and_invalid_values(self):
        env = {"MASSIVE_POLL_INTERVAL": "0.5", "SIMULATOR_INTERVAL": "fast", "SIMULATOR_SEED": "x"}
        with patch.dict(os.environ, env, clear=True):
            config = MarketDataConfig.from_env()
        assert config.massive_poll_interval == 2.0
        assert config.simulator_interval == 0.5
        assert config.simulator_seed is None

    def test_config_is_passed_to_sources(self):
        cache = PriceCache()
        massive = create_market_data_source(
            cache, MarketDataConfig(massive_api_key="k", massive_poll_interval=3.0)
        )
        assert isinstance(massive, MassiveDataSource)
        assert massive._interval == 3.0

        sim = create_market_data_source(
            cache, MarketDataConfig(simulator_interval=0.2, simulator_seed=9)
        )
        assert isinstance(sim, SimulatorDataSource)
        assert sim._interval == 0.2
        assert sim._seed == 9
