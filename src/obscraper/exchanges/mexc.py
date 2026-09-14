"""MEXC Spot.

MEXC hat den JSON-WebSocket 2025 abgekuendigt; der aktuelle Kanal
``spot@public.limit.depth.v3.api.pb@SYMBOL@N`` liefert ausschliesslich
Protobuf. Dekodiert wird ueber den abhaengigkeitsfreien Wire-Format-Leser in
``_protobuf.py`` (Begruendung und Grenzen dort dokumentiert).

Kontrollnachrichten (Subscribe-Bestaetigung, PONG) kommen weiterhin als
JSON-Textframes, Marktdaten als Binaerframes - beides wird hier getrennt
behandelt.

Faellt das Protobuf-Parsen wiederholt aus (etwa weil MEXC Feldnummern
umnummeriert), schaltet der Supervisor im transport=auto-Modus automatisch
auf REST-Polling um. Bei getaktetem Sampling ist das qualitativ nahezu
gleichwertig, nur mit etwas hoeherer Latenz.
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from ._protobuf import (
    ProtobufError,
    get_int,
    get_str,
    get_submessages,
    parse_message,
)
from .base import BookUpdate, ExchangeAdapter, Level, SymbolStatus, parse_levels

log = logging.getLogger(__name__)

_EXCHANGE_INFO = "https://api.mexc.com/api/v3/exchangeInfo"
_DEPTH = "https://api.mexc.com/api/v3/depth"
_LISTED_STATES = {"ENABLED", "TRADING", "1"}

# Feldnummern laut mexcdevelop/websocket-proto
_WRAP_CHANNEL = 1
_WRAP_SYMBOL = 3
_WRAP_CREATE_TIME = 5
_WRAP_SEND_TIME = 6
_WRAP_LIMIT_DEPTHS = 303  # PublicLimitDepthsV3Api  (Top-N-Snapshot)
_WRAP_AGGRE_DEPTHS = 313  # PublicAggreDepthsV3Api  (aggregiert/inkrementell)

_DEPTHS_ASKS = 1
_DEPTHS_BIDS = 2
_DEPTHS_VERSION = 4

_ITEM_PRICE = 1
_ITEM_QUANTITY = 2


class MexcAdapter(ExchangeAdapter):
    name = "mexc"
    WS_ENDPOINTS = ["wss://wbs-api.mexc.com/ws"]
    REST_BASE = "https://api.mexc.com"
    PARTIAL_DEPTHS = [5, 10, 20]
    KEEPALIVE_INTERVAL = 20.0  # Server trennt nach 30s Stille
    MIN_REST_INTERVAL_MS = 500

    def __init__(self, cfg, conn) -> None:
        super().__init__(cfg, conn)
        self.protobuf_errors = 0

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".upper()

    # -- WebSocket ---------------------------------------------------------

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "method": "SUBSCRIPTION",
                "params": [
                    f"spot@public.limit.depth.v3.api.pb@{s.native}@{self.effective_depth}"
                    for s in self.symbols
                ],
            }
        ]

    def keepalive_payload(self) -> Any | None:
        return {"method": "PING"}

    def decode_frame(self, raw: str | bytes) -> Any:
        # Marktdaten kommen binaer (Protobuf), Kontrollnachrichten als JSON-Text.
        # Der Binaerfall wird markiert statt sofort geparst, damit ein
        # fehlerhaftes Frame in parse() sauber abgefangen werden kann.
        if isinstance(raw, (bytes, bytearray)):
            return ("pb", bytes(raw))
        return super().decode_frame(raw)

    def reactive_reply(self, msg: Any) -> Any | None:
        # MEXC verlangt eine PONG-Antwort, falls der Server von sich aus pingt.
        if isinstance(msg, dict) and str(msg.get("msg", "")).upper() == "PING":
            return {"method": "PONG"}
        return None

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not (isinstance(msg, tuple) and len(msg) == 2 and msg[0] == "pb"):
            return []  # JSON-Kontrollframe (Subscribe-Ack, PONG)
        try:
            return self._parse_protobuf(msg[1])
        except ProtobufError as exc:
            self.protobuf_errors += 1
            self.last_error = f"ProtobufError: {exc}"
            if self.protobuf_errors in (1, 10, 100):
                log.warning(
                    "%s: Protobuf-Frame nicht lesbar (%dx): %s. Hat MEXC das "
                    "Schema geaendert? Bei anhaltendem Fehler greift im "
                    "transport=auto-Modus der REST-Fallback.",
                    self.name,
                    self.protobuf_errors,
                    exc,
                )
            return []

    def _parse_protobuf(self, data: bytes) -> list[BookUpdate]:
        wrapper = parse_message(data)

        body = None
        is_snapshot = True
        for field_no, snapshot in (
            (_WRAP_LIMIT_DEPTHS, True),
            (_WRAP_AGGRE_DEPTHS, False),
        ):
            subs = get_submessages(wrapper, field_no)
            if subs:
                body = subs[-1]
                is_snapshot = snapshot
                break
        if body is None:
            return []  # anderer Kanal im selben Wrapper - nicht unser Problem

        native_symbol = get_str(wrapper, _WRAP_SYMBOL)
        if not native_symbol:
            # Fallback: Symbol steht auch am Ende des Kanalnamens
            channel = get_str(wrapper, _WRAP_CHANNEL) or ""
            parts = channel.split("@")
            native_symbol = parts[-2] if len(parts) >= 2 else ""
        if not native_symbol:
            return []

        return [
            BookUpdate(
                symbol=native_symbol,
                bids=_levels(body, _DEPTHS_BIDS, self.effective_depth),
                asks=_levels(body, _DEPTHS_ASKS, self.effective_depth),
                ts_exchange=get_int(wrapper, _WRAP_SEND_TIME)
                or get_int(wrapper, _WRAP_CREATE_TIME),
                seq=_version_to_int(get_str(body, _DEPTHS_VERSION)),
                is_snapshot=is_snapshot,
            )
        ]

    # -- REST --------------------------------------------------------------

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _EXCHANGE_INFO)
        return {
            s["symbol"]
            for s in data.get("symbols", [])
            if str(s.get("status", "")).upper() in _LISTED_STATES
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


def _levels(body: dict, field_no: int, limit: int) -> list[Level]:
    out: list[Level] = []
    for item in get_submessages(body, field_no):
        price = get_str(item, _ITEM_PRICE)
        qty = get_str(item, _ITEM_QUANTITY)
        if price is None or qty is None:
            continue
        out.append((price, qty))
        if len(out) >= limit:
            break
    return out


def _version_to_int(version: str | None) -> int | None:
    try:
        return int(version) if version is not None else None
    except ValueError:
        return None
