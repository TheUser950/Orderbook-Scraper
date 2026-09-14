"""Bitget spot v2.

books1/books5/books15 are fixed snapshot channels; beyond those the adapter
switches to the incremental "books" channel (snapshot + deltas) and
reassembles the book locally.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_SYMBOLS = "https://api.bitget.com/api/v2/spot/public/symbols"
_ORDERBOOK = "https://api.bitget.com/api/v2/spot/market/orderbook"
_FIXED_CHANNELS = ((1, "books1"), (5, "books5"), (15, "books15"))


class BitgetAdapter(ExchangeAdapter):
    name = "bitget"
    WS_ENDPOINTS = ["wss://ws.bitget.com/v2/ws/public"]
    REST_BASE = "https://api.bitget.com"
    MAINTAINS_BOOK = True
    KEEPALIVE_INTERVAL = 20.0

    def __init__(self, cfg, conn) -> None:
        super().__init__(cfg, conn)
        self.channel = next(
            (ch for depth, ch in _FIXED_CHANNELS if depth >= self.effective_depth),
            "books",
        )

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "op": "subscribe",
                "args": [
                    {"instType": "SPOT", "channel": self.channel, "instId": s.native}
                    for s in self.symbols
                ],
            }
        ]

    def keepalive_payload(self) -> Any | None:
        return "ping"

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict) or "arg" not in msg or "data" not in msg:
            return []
        if msg["arg"].get("channel") != self.channel:
            return []
        native_symbol = msg["arg"].get("instId", "")
        # On Bitget books1/5/15 always carry action=="snapshot"; only the full
        # "books" channel uses "update" for deltas.
        is_snapshot = msg.get("action", "snapshot") == "snapshot"
        out = []
        for entry in msg["data"]:
            out.append(
                BookUpdate(
                    symbol=native_symbol,
                    bids=parse_levels(entry.get("bids")),
                    asks=parse_levels(entry.get("asks")),
                    ts_exchange=_to_int(entry.get("ts")),
                    is_snapshot=is_snapshot,
                )
            )
        return out

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _SYMBOLS)
        return {d["symbol"] for d in data.get("data", []) if d.get("status") == "online"}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _ORDERBOOK,
            params={
                "symbol": sym.native,
                "type": "step0",
                "limit": str(self.effective_depth),
            },
        )
        entry = data.get("data") or {}
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(entry.get("bids"), self.effective_depth),
            asks=parse_levels(entry.get("asks"), self.effective_depth),
            ts_exchange=_to_int(entry.get("ts")),
        )


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
