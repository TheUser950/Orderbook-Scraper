"""Bybit spot v5.

orderbook.{depth}.{symbol} delivers a snapshot followed by deltas, hence local
book maintenance. The channel is set to the next larger supported tier
(1/50/200) so enough levels arrive even at depth=20; the depth actually stored
stays exactly the configured one.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info"
_ORDERBOOK = "https://api.bybit.com/v5/market/orderbook"
_CHANNEL_DEPTHS = (1, 50, 200)


class BybitAdapter(ExchangeAdapter):
    name = "bybit"
    WS_ENDPOINTS = ["wss://stream.bybit.com/v5/public/spot"]
    REST_BASE = "https://api.bybit.com"
    MAINTAINS_BOOK = True
    KEEPALIVE_INTERVAL = 20.0

    def __init__(self, cfg, conn) -> None:
        super().__init__(cfg, conn)
        self.sub_depth = next(
            (d for d in _CHANNEL_DEPTHS if d >= self.effective_depth),
            _CHANNEL_DEPTHS[-1],
        )

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "op": "subscribe",
                "args": [f"orderbook.{self.sub_depth}.{s.native}" for s in self.symbols],
            }
        ]

    def keepalive_payload(self) -> Any | None:
        return {"op": "ping"}

    def parse(self, msg: Any) -> list[BookUpdate]:
        topic = msg.get("topic") if isinstance(msg, dict) else None
        if not topic or not topic.startswith("orderbook."):
            return []
        data = msg.get("data") or {}
        native_symbol = data.get("s", "")
        is_snapshot = msg.get("type") == "snapshot"
        seq = data.get("seq") or data.get("u")
        return [
            BookUpdate(
                symbol=native_symbol,
                bids=parse_levels(data.get("b")),
                asks=parse_levels(data.get("a")),
                ts_exchange=msg.get("ts"),
                seq=seq,
                is_snapshot=is_snapshot,
            )
        ]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _INSTRUMENTS, params={"category": "spot"})
        rows = (data.get("result") or {}).get("list", [])
        return {r["symbol"] for r in rows if r.get("status") == "Trading"}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _ORDERBOOK,
            params={
                "category": "spot",
                "symbol": sym.native,
                "limit": str(min(self.sub_depth, 200)),
            },
        )
        result = data.get("result") or {}
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(result.get("b"), self.effective_depth),
            asks=parse_levels(result.get("a"), self.effective_depth),
            seq=result.get("u"),
        )
