"""Gate.io Spot v4.

spot.order_book liefert bei jedem Push bereits ein vollstaendiges Top-N -
keine lokale Buchpflege noetig.
"""

from __future__ import annotations

import time
from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_PAIRS = "https://api.gateio.ws/api/v4/spot/currency_pairs"
_ORDERBOOK = "https://api.gateio.ws/api/v4/spot/order_book"
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
                # Feinstmoegliches Push-Intervall; das eigentliche
                # Aufzeichnungs-Raster bestimmt der Sampler unabhaengig davon.
                "payload": [s.native, str(self.effective_depth), "100ms"],
            }
            for s in self.symbols
        ]

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


def _to_ms(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
