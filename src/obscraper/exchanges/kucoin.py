"""KuCoin spot.

KuCoin does not hand out WebSocket endpoints statically but through a REST
bootstrap (POST /bullet-public) that returns a token, the server URL and the
required ping interval. Endpoint racing effectively drops out as a result (a
single candidate) - the bootstrap handshake itself dominates connection time
anyway.
"""

from __future__ import annotations

import itertools
import uuid
from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_BULLET = "https://api.kucoin.com/api/v1/bullet-public"
_SYMBOLS = "https://api.kucoin.com/api/v1/symbols"
_DEPTH20 = "https://api.kucoin.com/api/v1/market/orderbook/level2_20"
_DEPTH100 = "https://api.kucoin.com/api/v1/market/orderbook/level2_100"

_ids = itertools.count(1)


class KucoinAdapter(ExchangeAdapter):
    name = "kucoin"
    # Sentinel: the real URL comes dynamically from /bullet-public.
    WS_ENDPOINTS = ["kucoin-dynamic"]
    REST_BASE = "https://api.kucoin.com"
    PARTIAL_DEPTHS = [5, 50]

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}-{quote}".upper()

    async def ws_url(self, endpoint: str, session: aiohttp.ClientSession) -> str:
        async with session.post(
            _BULLET, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            body = await resp.json(content_type=None)
        if body.get("code") != "200000":
            raise RuntimeError(f"bullet-public failed: {body}")
        data = body["data"]
        token = data["token"]
        server = data["instanceServers"][0]
        self.KEEPALIVE_INTERVAL = max(5.0, server.get("pingInterval", 18000) / 1000 - 2)
        connect_id = uuid.uuid4().hex
        self._connect_id = connect_id
        return f"{server['endpoint']}?token={token}&connectId={connect_id}"

    def subscribe_payloads(self) -> list[Any]:
        depth = "level2Depth50" if self.effective_depth > 5 else "level2Depth5"
        return [
            {
                "id": str(next(_ids)),
                "type": "subscribe",
                "topic": f"/spotMarket/{depth}:{s.native}",
                "privateChannel": False,
                "response": True,
            }
            for s in self.symbols
        ]

    def keepalive_payload(self) -> Any | None:
        return {"id": str(next(_ids)), "type": "ping"}

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict) or msg.get("type") != "message":
            return []
        topic = msg.get("topic", "")
        if ":" not in topic:
            return []
        native_symbol = topic.rsplit(":", 1)[-1]
        data = msg.get("data") or {}
        bids = parse_levels(data.get("bids"), self.effective_depth)
        asks = parse_levels(data.get("asks"), self.effective_depth)
        if not bids and not asks:
            return []
        return [
            BookUpdate(
                symbol=native_symbol,
                bids=bids,
                asks=asks,
                ts_exchange=data.get("timestamp"),
            )
        ]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _SYMBOLS)
        return {d["symbol"] for d in data.get("data", []) if d.get("enableTrading")}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        url = _DEPTH100 if self.effective_depth > 20 else _DEPTH20
        data = await self.get_json(session, url, params={"symbol": sym.native})
        entry = data.get("data") or {}
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(entry.get("bids"), self.effective_depth),
            asks=parse_levels(entry.get("asks"), self.effective_depth),
            seq=entry.get("sequence"),
        )
