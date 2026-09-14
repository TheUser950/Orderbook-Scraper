"""BingX Spot.

Der Markt-WebSocket komprimiert jeden Frame per gzip, inklusive des
Text-Heartbeats: der Server schickt periodisch den (komprimierten) String
"Ping", worauf unkomprimiert mit "Pong" geantwortet werden muss.

Hinweis: BingX' oeffentliche Doku zu den exakten Feldnamen des Depth-Kanals
ist duenn/wandelt sich gelegentlich. parse() ist entsprechend defensiv
geschrieben - eine unerwartete Struktur fuehrt zu einer leeren Liste statt
einer Exception, der Supervisor reconnectet und die anderen Boersen laufen
unbeeintraechtigt weiter.
"""

from __future__ import annotations

import uuid
from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_SYMBOLS = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
_DEPTH = "https://open-api.bingx.com/openApi/spot/v1/market/depth"
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
        bids = parse_levels(data.get("bids"), self.effective_depth)
        asks = parse_levels(data.get("asks"), self.effective_depth)
        if not bids and not asks:
            return []
        return [BookUpdate(symbol=native_symbol, bids=bids, asks=asks)]

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
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(entry.get("bids"), self.effective_depth),
            asks=parse_levels(entry.get("asks"), self.effective_depth),
        )
