"""OKX spot.

For depth <= 5 the cheap ``books5`` channel is used (every message is already
a complete top-5 snapshot). For greater depths the incremental ``books``
channel (snapshot + deltas) is used and the book is reassembled locally. The
checksum OKX ships along is currently not verified - good enough for research
purposes, but worth keeping in mind (see README).
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import (
    BookUpdate,
    ExchangeAdapter,
    SymbolStatus,
    TradeUpdate,
    normalise_side,
    parse_levels,
)

_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments"
_BOOKS = "https://www.okx.com/api/v5/market/books"
_TRADES = "https://www.okx.com/api/v5/market/trades"


class OkxAdapter(ExchangeAdapter):
    name = "okx"
    WS_ENDPOINTS = [
        "wss://ws.okx.com:8443/ws/v5/public",
        "wss://wsaws.okx.com:8443/ws/v5/public",
    ]
    REST_BASE = "https://www.okx.com"
    MAINTAINS_BOOK = True  # any reachable depth, whether books5 or books
    KEEPALIVE_INTERVAL = 20.0

    def __init__(self, cfg, conn) -> None:
        super().__init__(cfg, conn)
        self.channel = "books5" if self.effective_depth <= 5 else "books"

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}-{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "op": "subscribe",
                "args": [
                    {"channel": self.channel, "instId": s.native} for s in self.symbols
                ],
            }
        ]

    def trade_subscribe_payloads(self) -> list[Any]:
        return [
            {
                "op": "subscribe",
                "args": [
                    {"channel": "trades", "instId": s.native} for s in self.symbols
                ],
            }
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not isinstance(msg, dict) or "arg" not in msg or "data" not in msg:
            return []
        if msg["arg"].get("channel") != "trades":
            return []
        native_symbol = msg["arg"].get("instId", "")
        return [_trade(e, native_symbol) for e in msg["data"]]

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        data = await self.get_json(
            session, _TRADES, params={"instId": sym.native, "limit": "100"}
        )
        return [_trade(e, sym.native) for e in data.get("data") or []]

    def keepalive_payload(self) -> Any | None:
        return "ping"

    def reactive_reply(self, msg: Any) -> Any | None:
        # OKX sends no server ping of its own, only "pong" in reply to ours.
        return None

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict) or "arg" not in msg or "data" not in msg:
            return []
        if msg["arg"].get("channel") != self.channel:
            return []
        native_symbol = msg["arg"].get("instId", "")
        action = msg.get("action", "snapshot")  # books5 carries no "action"
        out = []
        for entry in msg["data"]:
            out.append(
                BookUpdate(
                    symbol=native_symbol,
                    bids=parse_levels(entry.get("bids")),
                    asks=parse_levels(entry.get("asks")),
                    ts_exchange=_to_int(entry.get("ts")),
                    seq=_to_int(entry.get("seqId")),
                    # books5 carries neither field, so the continuity check
                    # simply does not engage there.
                    prev_seq=_to_int(entry.get("prevSeqId")),
                    is_snapshot=(action == "snapshot"),
                )
            )
        return out

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _INSTRUMENTS, params={"instType": "SPOT"})
        return {d["instId"] for d in data.get("data", [])}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _BOOKS,
            params={"instId": sym.native, "sz": str(self.effective_depth)},
        )
        rows = data.get("data") or []
        if not rows:
            return None
        entry = rows[0]
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(entry.get("bids"), self.effective_depth),
            asks=parse_levels(entry.get("asks"), self.effective_depth),
            ts_exchange=_to_int(entry.get("ts")),
        )


def _trade(e: dict, native_symbol: str) -> TradeUpdate:
    """OKX reports `side` as the taker side already - no inversion needed."""
    raw = e.get("side")
    return TradeUpdate(
        symbol=native_symbol,
        price=str(e.get("px")),
        qty=str(e.get("sz")),
        trade_id=str(e.get("tradeId")) if e.get("tradeId") is not None else None,
        ts_exchange=_to_int(e.get("ts")),
        side=normalise_side(raw),
        raw_side=raw,
    )


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
