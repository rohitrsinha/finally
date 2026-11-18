"""Tests for the SSE price stream."""

import asyncio
import json

import pytest

from app.market import PriceCache, create_stream_router
from app.market.stream import generate_price_events


class FakeRequest:
    """Minimal stand-in for a Starlette Request: disconnects after N checks."""

    client = None

    def __init__(self, polls_before_disconnect: int):
        self._remaining = polls_before_disconnect

    async def is_disconnected(self) -> bool:
        self._remaining -= 1
        return self._remaining < 0


def _data(frame: str) -> dict:
    line = next(line for line in frame.splitlines() if line.startswith("data: "))
    return json.loads(line[len("data: ") :])


@pytest.mark.asyncio
class TestGeneratePriceEvents:
    async def test_retry_then_snapshot_then_heartbeat(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        frames = [
            f
            async for f in generate_price_events(
                cache, FakeRequest(3), interval=0.01, heartbeat=0.0
            )
        ]
        assert frames[0] == "retry: 1000\n\n"
        assert frames[1].startswith(f"id: {cache.version}\nevent: prices\n")
        assert _data(frames[1])["AAPL"]["price"] == 190.0
        assert frames[2] == ": ping\n\n"  # nothing changed since -> heartbeat

    async def test_no_heartbeat_before_interval(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        frames = [
            f
            async for f in generate_price_events(
                cache, FakeRequest(3), interval=0.01, heartbeat=60.0
            )
        ]
        assert frames == frames[:2]  # retry + one snapshot, no pings

    async def test_sends_new_frame_when_cache_changes(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        gen = generate_price_events(cache, FakeRequest(10), interval=0.01, heartbeat=60.0)
        assert await anext(gen) == "retry: 1000\n\n"
        assert _data(await anext(gen))["AAPL"]["price"] == 190.0
        cache.update("AAPL", 191.0)
        cache.remove("MSFT")  # unknown ticker: no version bump, no extra frame
        frame = await asyncio.wait_for(anext(gen), timeout=1)
        assert _data(frame)["AAPL"]["price"] == 191.0
        assert _data(frame)["AAPL"]["direction"] == "up"
        await gen.aclose()

    async def test_removed_ticker_disappears(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("MSFT", 420.0)
        gen = generate_price_events(cache, FakeRequest(10), interval=0.01, heartbeat=60.0)
        await anext(gen)
        assert set(_data(await anext(gen))) == {"AAPL", "MSFT"}
        cache.remove("MSFT")
        assert set(_data(await asyncio.wait_for(anext(gen), timeout=1))) == {"AAPL"}
        await gen.aclose()

    async def test_empty_cache_sends_empty_snapshot(self):
        frames = [
            f
            async for f in generate_price_events(
                PriceCache(), FakeRequest(1), interval=0.01, heartbeat=60.0
            )
        ]
        assert _data(frames[1]) == {}


def test_router_factory_is_independent():
    r1 = create_stream_router(PriceCache())
    r2 = create_stream_router(PriceCache())
    assert r1 is not r2
    assert len(r1.routes) == len(r2.routes) == 1
    assert r1.routes[0].path == "/api/stream/prices"
