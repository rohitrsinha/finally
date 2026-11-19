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
