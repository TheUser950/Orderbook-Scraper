"""Coinbase Exchange (formerly Coinbase Pro) spot.

level2_batch is the only unauthenticated channel with order book depth: an
initial "snapshot", then "l2update" deltas in Coinbase's own
[side, price, size] format. Book maintenance is therefore necessarily local.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import aiohttp

from .base import (
    BookUpdate,
    ExchangeAdapter,
    Level,
    SymbolStatus,
    TradeUpdate,
    invert_side,
    normalise_side,
    parse_levels,
)

_PRODUCTS = "https://api.exchange.coinbase.com/products"
_BOOK = "https://api.exchange.coinbase.com/products/{}/book"
_TRADES = "https://api.exchange.coinbase.com/products/{}/trades"

# Coinbase wants a recognisable User-Agent; without one it occasionally 403s.
_HEADERS = {"User-Agent": "orderbook-scraper/0.1 (research use)"}


class CoinbaseAdapter(ExchangeAdapter):
    name = "coinbase"
    WS_ENDPOINTS = ["wss://ws-feed.exchange.coinbase.com"]
    REST_BASE = "https://api.exchange.coinbase.com"
    MAINTAINS_BOOK = True

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}-{quote}".upper()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "type": "subscribe",
                "product_ids": [s.native for s in self.symbols],
                "channels": ["level2_batch"],
            }
        ]

    def trade_subscribe_payloads(self) -> list[Any]:
        return [
            {
                "type": "subscribe",
                "product_ids": [s.native for s in self.symbols],
                "channels": ["matches"],
            }
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not isinstance(msg, dict):
            return []
        # "last_match" is the one-off replay Coinbase sends on subscribe.
        if msg.get("type") not in ("match", "last_match"):
            return []
        native_symbol = msg.get("product_id", "")
        if not native_symbol:
            return []
        return [_trade(msg, native_symbol)]

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        rows = await self.get_json(
            session, _TRADES.format(sym.native), params={"limit": "100"}, headers=_HEADERS
        )
        return [_trade(e, sym.native) for e in rows]

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict):
            return []
        msg_type = msg.get("type")
        native_symbol = msg.get("product_id", "")
        if not native_symbol:
            return []

        if msg_type == "snapshot":
            return [
                BookUpdate(
                    symbol=native_symbol,
                    bids=parse_levels(msg.get("bids")),
                    asks=parse_levels(msg.get("asks")),
                    is_snapshot=True,
                )
            ]
        if msg_type == "l2update":
            bids: list[Level] = []
            asks: list[Level] = []
            for side, price, size in msg.get("changes", []):
                (bids if side == "buy" else asks).append((price, size))
            if not bids and not asks:
                return []
            return [
                BookUpdate(symbol=native_symbol, bids=bids, asks=asks, is_snapshot=False)
            ]
        return []

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _PRODUCTS, headers=_HEADERS)
        return {d["id"] for d in data if d.get("status") == "online"}

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session, _BOOK.format(sym.native), params={"level": "2"}, headers=_HEADERS
        )
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(data.get("bids"), self.effective_depth),
            asks=parse_levels(data.get("asks"), self.effective_depth),
            seq=data.get("sequence"),
        )


def _trade(e: dict, native_symbol: str) -> TradeUpdate:
    """Coinbase reports the MAKER side, so it has to be inverted.

    From the Exchange docs: "the side field indicates the maker order side".
    A match with side='sell' means the resting order was a sell, so the
    incoming aggressor was a *buyer*. Every other exchange here reports the
    taker directly, so without this inversion Coinbase's order flow would come
    out exactly backwards - and nothing about the data would look wrong.

    raw_side keeps the original value so the inversion stays auditable.
    """
    raw = e.get("side")
    maker = normalise_side(raw)
    ts = e.get("time")
    return TradeUpdate(
        symbol=native_symbol,
        price=str(e.get("price")),
        qty=str(e.get("size")),
        trade_id=str(e.get("trade_id")) if e.get("trade_id") is not None else None,
        ts_exchange=_iso_to_ms(ts),
        side=invert_side(maker) if maker else None,
        raw_side=f"maker={raw}",
    )


def _iso_to_ms(value: Any) -> int | None:
    """Coinbase timestamps are ISO-8601 strings, not epoch milliseconds."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(
            datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
        )
    except ValueError:
        return None
