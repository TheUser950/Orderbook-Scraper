"""BingX spot.

The market WebSocket gzip-compresses every frame, including the text
heartbeat: the server periodically sends the (compressed) string "Ping", which
has to be answered with an uncompressed "Pong".

Note: the public documentation of the exact field names on the depth channel
is thin and changes occasionally. parse() is written defensively because of
that - an unexpected structure yields an empty list rather than an exception,
the supervisor reconnects, and the other exchanges keep running unaffected.
"""

from __future__ import annotations

import uuid
from typing import Any

import aiohttp

from .base import (
    BookUpdate,
    ExchangeAdapter,
    SymbolStatus,
    TradeUpdate,
    parse_levels,
    sort_levels,
)

_SYMBOLS = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
_DEPTH = "https://open-api.bingx.com/openApi/spot/v1/market/depth"
_TRADES = "https://open-api.bingx.com/openApi/spot/v1/market/trades"
_PARTIAL_DEPTHS = [5, 10, 20, 50, 100]


class BingXAdapter(ExchangeAdapter):
    name = "bingx"
    WS_ENDPOINTS = ["wss://open-api-ws.bingx.com/market"]
    REST_BASE = "https://open-api.bingx.com"
    PARTIAL_DEPTHS = _PARTIAL_DEPTHS
    GZIP_FRAMES = True

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}-{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "id": str(uuid.uuid4()),
                "reqType": "sub",
                "dataType": f"{s.native}@depth{self.effective_depth}",
            }
            for s in self.symbols
        ]

    def trade_subscribe_payloads(self) -> list[Any]:
        return [
            {
                "id": str(uuid.uuid4()),
                "reqType": "sub",
                "dataType": f"{s.native}@trade",
            }
            for s in self.symbols
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not isinstance(msg, dict):
            return []
        data_type = msg.get("dataType", "")
        if not data_type.endswith("@trade"):
            return []
        native_symbol = data_type.split("@", 1)[0]
        data = msg.get("data")
        rows = data if isinstance(data, list) else [data]
        return [_trade(e, native_symbol) for e in rows if isinstance(e, dict)]

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        data = await self.get_json(
            session, _TRADES, params={"symbol": sym.native, "limit": "100"}
        )
        rows = data.get("data") or []
        return [_rest_trade(e, sym.native) for e in rows if isinstance(e, dict)]

    def reactive_reply(self, msg: Any) -> Any | None:
        if isinstance(msg, str) and msg.strip() == "Ping":
            return "Pong"
        return None

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict):
            return []
        data_type = msg.get("dataType", "")
        if "@depth" not in data_type:
            return []
        native_symbol = data_type.split("@", 1)[0]
        data = msg.get("data") or {}
        bids, asks = self._ordered(data.get("bids"), data.get("asks"))
        if not bids and not asks:
            return []
        return [BookUpdate(symbol=native_symbol, bids=bids, asks=asks)]

    def _ordered(self, raw_bids, raw_asks) -> tuple[list, list]:
        """BingX delivers asks worst-price-first, so sort before truncating.

        Verified against the live feed: bids arrive descending as usual, but
        asks arrive descending too, which puts the best ask last. Truncating
        first would therefore keep the worst levels and drop the best ones.
        """
        bids = sort_levels(parse_levels(raw_bids), descending=True)
        asks = sort_levels(parse_levels(raw_asks), descending=False)
        return bids[: self.effective_depth], asks[: self.effective_depth]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _SYMBOLS)
        rows = (data.get("data") or {}).get("symbols", [])
        return {r["symbol"] for r in rows if str(r.get("status")) in ("1", "true")}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        depth = next((d for d in _PARTIAL_DEPTHS if d >= self.effective_depth), 100)
        data = await self.get_json(
            session, _DEPTH, params={"symbol": sym.native, "limit": str(depth)}
        )
        entry = data.get("data") or {}
        bids, asks = self._ordered(entry.get("bids"), entry.get("asks"))
        return BookUpdate(symbol=sym.native, bids=bids, asks=asks)


def _trade(e: dict, native_symbol: str) -> TradeUpdate:
    """BingX trades. The aggressor side is deliberately left unknown.

    The frame carries an `m` flag that looks like Binance's "buyer is maker",
    and the field names were confirmed against the live feed. But interpreting
    it that way does not survive validation: measured over ~1400 trades, `m`
    shows no relationship to where the trade printed (buy and sell produce
    near-identical distributions across bid/ask), and a book-independent tick
    test scores 47.8% - indistinguishable from random. Inverting it does not
    help either; the flag simply carries no directional information we can
    confirm.

    Writing a side we cannot validate is worse than writing none: it would look
    perfectly plausible while silently corrupting any order-flow analysis. So
    `side` stays NULL and the flag is preserved verbatim in `raw_side`, ready
    to be reinterpreted if BingX documents it or the meaning becomes clear.
    """
    return TradeUpdate(
        symbol=native_symbol,
        price=str(e.get("p")),
        qty=str(e.get("q")),
        trade_id=str(e["t"]) if e.get("t") is not None else None,
        ts_exchange=e.get("T"),
        side=None,
        raw_side=f"m={e.get('m')}",
    )


def _rest_trade(e: dict, native_symbol: str) -> TradeUpdate:
    """REST shape: id/price/qty/time/buyerMaker, none of the WS field names.

    Reusing the WS parser here lost the trade id, so nothing deduplicated and
    every poll re-inserted the whole window. The side stays unknown for the
    same reason as on the WebSocket.
    """
    return TradeUpdate(
        symbol=native_symbol,
        price=str(e.get("price")),
        qty=str(e.get("qty")),
        trade_id=str(e["id"]) if e.get("id") is not None else None,
        ts_exchange=e.get("time"),
        side=None,
        raw_side=f"buyerMaker={e.get('buyerMaker')}",
    )
