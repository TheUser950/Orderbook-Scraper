"""Coinbase Exchange (ehem. Coinbase Pro) Spot.

level2_batch ist der einzige unauthentifizierte Kanal mit Orderbook-Tiefe:
ein initiales "snapshot", danach "l2update"-Deltas im Coinbase-eigenen
[side, price, size]-Format. Buchpflege daher zwingend lokal.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, Level, SymbolStatus, parse_levels

_PRODUCTS = "https://api.exchange.coinbase.com/products"
_BOOK = "https://api.exchange.coinbase.com/products/{}/book"

# Coinbase verlangt einen erkennbaren User-Agent; ohne ihn kommt gelegentlich 403.
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
