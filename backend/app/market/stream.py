"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    Builds a new router on every call (no module-level state), so calling it
    more than once (e.g. in tests) never registers duplicate routes.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Each frame is a named `prices` event whose data is a full snapshot of
        all tracked tickers:

            id: 1042
            event: prices
            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Clients listen with `source.addEventListener("prices", ...)`.
        """
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects.

    Sends a full price snapshot whenever the cache version changes (checked
    every `interval` seconds), and a `: ping` comment after `heartbeat`
    seconds without data so idle connections aren't closed by proxies.
    """
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    loop = asyncio.get_running_loop()
    last_version = -1
    last_sent = loop.time()
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                yield f"id: {version}\nevent: prices\ndata: {payload}\n\n"
                last_sent = loop.time()
            elif loop.time() - last_sent >= heartbeat:
                yield ": ping\n\n"  # SSE comment: ignored by EventSource
                last_sent = loop.time()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client_ip)
