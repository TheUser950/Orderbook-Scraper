"""OKX Spot.

Fuer Tiefe <= 5 wird der guenstige ``books5``-Kanal genutzt (jede Nachricht
ist bereits ein vollstaendiges Top-5-Snapshot). Fuer groessere Tiefen kommt
der inkrementelle ``books``-Kanal (Snapshot + Deltas) zum Einsatz und das Buch
wird lokal zusammengesetzt. Die von OKX mitgelieferte Checksumme wird aktuell
nicht verifiziert - fuer Forschungszwecke ausreichend, aber im Auge zu
behalten (siehe README).
"""

from __future__ import annotations

from typing import Any

import aiohttp

from .base import BookUpdate, ExchangeAdapter, SymbolStatus, parse_levels

_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments"
_BOOKS = "https://www.okx.com/api/v5/market/books"


class OkxAdapter(ExchangeAdapter):
    name = "okx"
    WS_ENDPOINTS = [
        "wss://ws.okx.com:8443/ws/v5/public",
        "wss://wsaws.okx.com:8443/ws/v5/public",
    ]
    REST_BASE = "https://www.okx.com"
    MAINTAINS_BOOK = True  # jede erreichbare Tiefe, egal ob books5 oder books
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

    def keepalive_payload(self) -> Any | None:
        return "ping"

    def reactive_reply(self, msg: Any) -> Any | None:
        return None  # OKX schickt selbst kein Server-Ping, nur "pong" auf unseres

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not isinstance(msg, dict) or "arg" not in msg or "data" not in msg:
            return []
        if msg["arg"].get("channel") != self.channel:
            return []
        native_symbol = msg["arg"].get("instId", "")
        action = msg.get("action", "snapshot")  # books5 hat kein "action"
        out = []
        for entry in msg["data"]:
            out.append(
                BookUpdate(
                    symbol=native_symbol,
                    bids=parse_levels(entry.get("bids")),
                    asks=parse_levels(entry.get("asks")),
                    ts_exchange=_to_int(entry.get("ts")),
                    seq=_to_int(entry.get("seqId")),
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
            session, _BOOKS, params={"instId": sym.native, "sz": str(self.effective_depth)}
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


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
