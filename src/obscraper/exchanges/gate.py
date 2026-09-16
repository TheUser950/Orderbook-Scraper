"""Gate.io spot v4.

spot.order_book already delivers a complete top-N on every push - no local
book maintenance needed.
"""

from __future__ import annotations

import time
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

_PAIRS = "https://api.gateio.ws/api/v4/spot/currency_pairs"
_ORDERBOOK = "https://api.gateio.ws/api/v4/spot/order_book"
_TRADES = "https://api.gateio.ws/api/v4/spot/trades"
_PARTIAL_DEPTHS = [5, 10, 20, 50, 100]


class GateAdapter(ExchangeAdapter):
    name = "gate"
    WS_ENDPOINTS = ["wss://api.gateio.ws/ws/v4/"]
    REST_BASE = "https://api.gateio.ws"
    PARTIAL_DEPTHS = _PARTIAL_DEPTHS
    KEEPALIVE_INTERVAL = 20.0

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}_{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "time": int(time.time()),
                "channel": "spot.order_book",
                "event": "subscribe",
                # Finest available push interval; the actual recording grid is
                # decided independently by the sampler.
                "payload": [s.native, str(self.effective_depth), "100ms"],
            }
            for s in self.symbols
        ]

    def trade_subscribe_payloads(self) -> list[Any]:
        return [
            {
                "time": int(time.time()),
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [s.native for s in self.symbols],
            }
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not isinstance(msg, dict) or msg.get("channel") != "spot.trades":
            return []
        if msg.get("event") != "update":
            return []
        result = msg.get("result")
        rows = result if isinstance(result, list) else [result]
        return [_trade(e) for e in rows if isinstance(e, dict)]

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        rows = await self.get_json(
            session, _TRADES, params={"currency_pair": sym.native, "limit": "100"}
        )
        return [_trade(e) for e in rows]

    def keepalive_payload(self) -> Any | None:
        return {"time": int(time.time()), "channel": "spot.ping"}

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict) or msg.get("channel") != "spot.order_book":
            return []
        if msg.get("event") != "update":
            return []
        result = msg.get("result") or {}
        native_symbol = result.get("s", "")
        if not native_symbol:
            return []
        return [
            BookUpdate(
                symbol=native_symbol,
                bids=parse_levels(result.get("bids"), self.effective_depth),
                asks=parse_levels(result.get("asks"), self.effective_depth),
                ts_exchange=_to_ms(result.get("t")),
                seq=result.get("lastUpdateId"),
            )
        ]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _PAIRS)
        return {d["id"] for d in data if d.get("trade_status") == "tradable"}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _ORDERBOOK,
            params={"currency_pair": sym.native, "limit": str(self.effective_depth)},
        )
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(data.get("bids"), self.effective_depth),
            asks=parse_levels(data.get("asks"), self.effective_depth),
            seq=data.get("id"),
        )


def _trade(e: dict) -> TradeUpdate:
    """Gate's `side` is the taker side already.

    `create_time_ms` is a string with a fractional part on this endpoint, so it
    is truncated at the decimal point before converting.
    """
    raw = e.get("side")
    ts = e.get("create_time_ms") or e.get("create_time")
    return TradeUpdate(
        symbol=e.get("currency_pair", ""),
        price=str(e.get("price")),
        qty=str(e.get("amount")),
        trade_id=str(e.get("id")) if e.get("id") is not None else None,
        ts_exchange=_to_ms(str(ts).split(".")[0] if ts is not None else None),
        side=normalise_side(raw),
        raw_side=raw,
    )


def _to_ms(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
