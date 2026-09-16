"""Binance spot.

Uses the "partial book depth" streams (<symbol>@depth<N>@100ms), which already
deliver a finished top-N - no local book maintenance needed. Multiple symbols
run over one combined stream on a single connection. Several publicly
reachable hosts make Binance a good candidate for endpoint racing.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import (
    BookUpdate,
    ExchangeAdapter,
    SymbolStatus,
    TradeUpdate,
    parse_levels,
    taker_from_buyer_maker,
)

_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
_DEPTH = "https://api.binance.com/api/v3/depth"
_TRADES = "https://api.binance.com/api/v3/trades"


class BinanceAdapter(ExchangeAdapter):
    name = "binance"
    WS_ENDPOINTS = [
        "wss://stream.binance.com:9443",
        "wss://stream.binance.com:443",
        "wss://data-stream.binance.vision",
    ]
    REST_BASE = "https://api.binance.com"
    PARTIAL_DEPTHS = [5, 10, 20]
    KEEPALIVE_INTERVAL = None  # the websockets lib answers Binance's pings itself

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".upper()

    async def ws_url(self, endpoint: str, session: aiohttp.ClientSession) -> str:
        # Binance selects streams through the URL rather than a subscribe
        # message, so the trade streams have to be named here too.
        streams = [
            f"{s.native.lower()}@depth{self.effective_depth}@100ms"
            for s in self.symbols
        ]
        if self.collect_trades:
            streams += [f"{s.native.lower()}@trade" for s in self.symbols]
        return f"{endpoint}/stream?streams={'/'.join(streams)}"

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict):
            return []
        stream = msg.get("stream")
        data = msg.get("data")
        if not stream or not isinstance(data, dict):
            return []
        native_symbol = stream.split("@", 1)[0].upper()
        bids = parse_levels(data.get("bids"), self.effective_depth)
        asks = parse_levels(data.get("asks"), self.effective_depth)
        if not bids and not asks:
            return []
        return [
            BookUpdate(
                symbol=native_symbol,
                bids=bids,
                asks=asks,
                seq=data.get("lastUpdateId"),
                is_snapshot=True,
            )
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not isinstance(msg, dict):
            return []
        stream = msg.get("stream") or ""
        data = msg.get("data")
        if not stream.endswith("@trade") or not isinstance(data, dict):
            return []
        return [_trade(data, stream.split("@", 1)[0].upper())]

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        rows = await self.get_json(
            session, _TRADES, params={"symbol": sym.native, "limit": "100"}
        )
        return [_trade(r, sym.native, rest=True) for r in rows]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _EXCHANGE_INFO)
        return {
            s["symbol"] for s in data.get("symbols", []) if s.get("status") == "TRADING"
        }

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _DEPTH,
            params={"symbol": sym.native, "limit": str(self.effective_depth)},
        )
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(data.get("bids"), self.effective_depth),
            asks=parse_levels(data.get("asks"), self.effective_depth),
            seq=data.get("lastUpdateId"),
        )


def _trade(r: dict, native_symbol: str, rest: bool = False) -> TradeUpdate:
    """Binance reports `m` = "the buyer was the maker", so the taker is the
    opposite side. Same field name in the WS payload and the REST response."""
    buyer_is_maker = r.get("m") if not rest else r.get("isBuyerMaker")
    return TradeUpdate(
        symbol=native_symbol,
        price=str(r.get("p") if not rest else r.get("price")),
        qty=str(r.get("q") if not rest else r.get("qty")),
        trade_id=str(r.get("t") if not rest else r.get("id")),
        ts_exchange=r.get("T") if not rest else r.get("time"),
        side=taker_from_buyer_maker(buyer_is_maker),
        raw_side=f"m={buyer_is_maker}",
    )
