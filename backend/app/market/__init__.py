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
