"""HTX (ehem. Huobi) Spot.

Der Kanal market.$symbol.depth.step0 liefert bei jedem Push das komplette
Buch (bis 150 Level) unaggregiert - kein Snapshot/Delta-Handling noetig,
nur auf die gewuenschte Tiefe kappen. Frames kommen binaer, gzip-komprimiert;
HTX schickt periodisch ein Ping, auf das aktiv geantwortet werden muss.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_SYMBOLS = "https://api.huobi.pro/v1/common/symbols"
_DEPTH = "https://api.huobi.pro/market/depth"
_MAX_STEP0_DEPTH = 150


class HtxAdapter(ExchangeAdapter):
    name = "htx"
    WS_ENDPOINTS = ["wss://api.huobi.pro/ws", "wss://api-aws.huobi.pro/ws"]
    REST_BASE = "https://api.huobi.pro"
    GZIP_FRAMES = True

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".lower()

    def subscribe_payloads(self) -> list[Any]:
        return [
            {"sub": f"market.{s.native}.depth.step0", "id": f"obs-{i}"}
            for i, s in enumerate(self.symbols)
        ]

    def reactive_reply(self, msg: Any) -> Any | None:
        if isinstance(msg, dict) and "ping" in msg:
            return {"pong": msg["ping"]}
        return None

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict):
            return []
        ch = msg.get("ch", "")
        if not ch.startswith("market.") or not ch.endswith(".depth.step0"):
            return []
        native_symbol = ch.split(".")[1]
        tick = msg.get("tick") or {}
        return [
            BookUpdate(
                symbol=native_symbol,
                bids=parse_levels(tick.get("bids"), self.effective_depth),
                asks=parse_levels(tick.get("asks"), self.effective_depth),
                ts_exchange=msg.get("ts"),
                seq=tick.get("version"),
            )
        ]

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _SYMBOLS)
        return {
            d["symbol"] for d in data.get("data", []) if d.get("state") == "online"
        }

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        # Die REST-Variante von step0 kappt bei 20 Leveln.
        depth_param = 20 if self.effective_depth > 5 else 5
        data = await self.get_json(
            session,
            _DEPTH,
            params={"symbol": sym.native, "type": "step0", "depth": str(depth_param)},
        )
        tick = data.get("tick") or {}
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(tick.get("bids"), self.effective_depth),
            asks=parse_levels(tick.get("asks"), self.effective_depth),
            ts_exchange=data.get("ts"),
            seq=tick.get("version"),
        )
