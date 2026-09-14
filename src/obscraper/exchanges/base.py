"""Gemeinsame Basis aller Boersen-Adapter.

Kernidee: Ein Adapter haelt ein *lebendes* Top-N-Buch je Symbol im Speicher.
Wie er das tut - Push eines fertigen Top-N, Snapshot plus Deltas, oder
REST-Polling - ist seine Sache und fuer den Rest des Programms unsichtbar.
Der Sampler greift dieses Buch im Wall-Clock-Takt ab. Dadurch liefert eine
Boerse im REST-Fallback Daten derselben Form wie eine per WebSocket, und eine
haengende Verbindung blockiert nie die anderen neun.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import orjson
from websockets.asyncio.client import connect as ws_connect

from ..config import ConnectionConfig, ExchangeConfig
from ..models import (
    FLAG_CROSSED,
    FLAG_PARTIAL,
    FLAG_STALE,
    BookState,
    IncrementalBook,
    Level,
    OrderBookSnapshot,
    is_crossed,
    now_ms,
)

log = logging.getLogger(__name__)


@dataclass(slots=True)
class BookUpdate:
    """Ein geparster Orderbook-Frame, boersenunabhaengig."""

    symbol: str  # native Schreibweise der Boerse
    bids: list[Level]
    asks: list[Level]
    ts_exchange: int | None = None
    seq: int | None = None
    is_snapshot: bool = True


@dataclass(slots=True)
class SymbolStatus:
    canonical: str
    native: str
    listed: bool | None = None  # None = nicht geprueft
    note: str = ""


def parse_levels(raw: Any, limit: int | None = None) -> list[Level]:
    """Normalisiert die diversen Level-Formate auf [(preis, menge), ...].

    Deckt ``[["1.0","2.0"], ...]`` (die meisten), ``[["1.0","2.0","0","1"]]``
    (OKX/Bitget mit Zusatzfeldern) und ``[[1.0, 2.0]]`` (numerisch, z.B. HTX)
    ab. Zahlen werden per repr in Strings ueberfuehrt, damit der Schreibpfad
    durchgaengig stringbasiert bleibt.
    """
    out: list[Level] = []
    if not raw:
        return out
    for entry in raw:
        if not entry or len(entry) < 2:
            continue
        price, qty = entry[0], entry[1]
        out.append(
            (
                price if isinstance(price, str) else repr(price),
                qty if isinstance(qty, str) else repr(qty),
            )
        )
        if limit is not None and len(out) >= limit:
            break
    return out


class ExchangeAdapter(ABC):
    # -- Deklaration je Boerse --------------------------------------------
    name: str = ""
    WS_ENDPOINTS: list[str] = []
    REST_BASE: str = ""
    # Nativ per Push verfuegbare Top-N-Stufen. Leer = Buch wird lokal gepflegt.
    PARTIAL_DEPTHS: list[int] = []
    # True, wenn der Kanal Snapshot + Deltas liefert und lokal zusammengesetzt
    # werden muss (OKX, Bitget, Bybit, Coinbase).
    MAINTAINS_BOOK: bool = False
    GZIP_FRAMES: bool = False
    KEEPALIVE_INTERVAL: float | None = None
    SUPPORTS_WS: bool = True
    SUPPORTS_REST: bool = True
    # Maximale REST-Polling-Frequenz, um nicht in Rate-Limits zu laufen.
    MIN_REST_INTERVAL_MS: int = 200

    def __init__(self, cfg: ExchangeConfig, conn: ConnectionConfig) -> None:
        self.cfg = cfg
        self.conn = conn
        self.requested_depth = cfg.depth
        self.effective_depth = self._resolve_depth()

        self.symbols: list[SymbolStatus] = []
        for canonical in cfg.symbols:
            native = cfg.symbol_override.get(canonical) or self.native_symbol(canonical)
            self.symbols.append(SymbolStatus(canonical, native))
        self.by_native: dict[str, str] = {s.native: s.canonical for s in self.symbols}

        self.books: dict[str, BookState] = {}
        self.inc: dict[str, IncrementalBook] = {
            s.canonical: IncrementalBook() for s in self.symbols
        }
        self.last_update_mono: dict[str, float] = {}

        self.transport: str = "ws" if self.SUPPORTS_WS else "rest"
        self.endpoint: str | None = None
        self.connected = False
        self.reconnects = 0
        self.messages = 0
        self.last_error: str | None = None

    # -- Von Unterklassen zu implementieren --------------------------------

    @classmethod
    @abstractmethod
    def native_symbol(cls, canonical: str) -> str:
        """'ETH/USDC' -> boersen-eigene Schreibweise."""

    @abstractmethod
    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        """Native Symbole, die die Boerse aktuell im Spot-Handel fuehrt."""

    @abstractmethod
    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        """Einmaliges Orderbook per REST."""

    def parse(self, msg: Any) -> list[BookUpdate]:
        """Frame -> Updates. Leere Liste fuer alles Uninteressante."""
        return []

    async def ws_url(self, endpoint: str, session: aiohttp.ClientSession) -> str:
        """Erlaubt Adaptern, die URL dynamisch zu bilden (Streams, Token)."""
        return endpoint

    def subscribe_payloads(self) -> list[Any]:
        """Nachrichten, die direkt nach dem Verbinden gesendet werden."""
        return []

    def keepalive_payload(self) -> Any | None:
        """Periodisch zu sendender Heartbeat (Intervall: KEEPALIVE_INTERVAL)."""
        return None

    def reactive_reply(self, msg: Any) -> Any | None:
        """Antwort auf ein server-initiiertes Ping. None = kein Ping."""
        return None

    # -- Tiefen-Aufloesung -------------------------------------------------

    def _resolve_depth(self) -> int:
        """Bildet die Wunschtiefe auf eine von der Boerse angebotene Stufe ab."""
        if self.MAINTAINS_BOOK or not self.PARTIAL_DEPTHS:
            # Lokal gepflegtes Buch: jede Tiefe ist erreichbar.
            return self.requested_depth

        supported = sorted(self.PARTIAL_DEPTHS)
        want = self.requested_depth
        if want in supported:
            return want

        policy = self.cfg.depth_policy
        higher = [s for s in supported if s > want]
        lower = [s for s in supported if s < want]

        if policy == "at_least":
            chosen = higher[0] if higher else supported[-1]
        elif policy == "at_most":
            chosen = lower[-1] if lower else supported[0]
        else:
            chosen = min(supported, key=lambda s: (abs(s - want), s))

        log.info(
            "%s: Tiefe %d nicht verfuegbar (unterstuetzt: %s) -> %d [%s]",
            self.name,
            want,
            supported,
            chosen,
            policy,
        )
        return chosen

    # -- Frame-Verarbeitung ------------------------------------------------

    def decode_frame(self, raw: str | bytes) -> Any:
        if isinstance(raw, (bytes, bytearray)):
            if self.GZIP_FRAMES:
                raw = self._decompress(bytes(raw))
            if isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw).decode("utf-8", "replace")
        text = raw.strip()
        if text[:1] in ("{", "["):
            try:
                return orjson.loads(text)
            except orjson.JSONDecodeError:
                return text
        return text

    @staticmethod
    def _decompress(data: bytes) -> bytes:
        for attempt in (
            lambda: gzip.decompress(data),
            lambda: zlib.decompress(data),
            lambda: zlib.decompress(data, -zlib.MAX_WBITS),
        ):
            try:
                return attempt()
            except (OSError, zlib.error):
                continue
        return data

    async def get_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        timeout = aiohttp.ClientTimeout(total=self.conn.rest_timeout_s)
        async with session.get(
            url, params=params, timeout=timeout, headers=headers
        ) as resp:
            body = await resp.read()
            if resp.status != 200:
                raise RuntimeError(
                    f"HTTP {resp.status} von {url}: {body[:200].decode('utf-8', 'replace')}"
                )
            return orjson.loads(body)

    @staticmethod
    async def _send(ws: Any, payload: Any) -> None:
        if isinstance(payload, (dict, list)):
            await ws.send(orjson.dumps(payload).decode())
        else:
            await ws.send(str(payload))

    # -- WebSocket ---------------------------------------------------------

    async def run_ws(
        self, endpoint: str, session: aiohttp.ClientSession, on_connect=None
    ) -> None:
        """Verbindet, abonniert und laeuft, bis die Verbindung abbricht."""
        url = await self.ws_url(endpoint, session)
        async with ws_connect(
            url,
            ping_interval=self.conn.ws_ping_interval_s,
            ping_timeout=self.conn.ws_ping_interval_s * 2,
            open_timeout=self.conn.latency_probe_timeout_s * 2,
            close_timeout=5,
            max_size=16 * 1024 * 1024,
        ) as ws:
            self.endpoint = endpoint
            self.connected = True
            self.transport = "ws"
            for canonical in self.inc:
                self.inc[canonical].reset()
            for payload in self.subscribe_payloads():
                await self._send(ws, payload)
            if on_connect is not None:
                on_connect(endpoint)

            keepalive: asyncio.Task | None = None
            if self.KEEPALIVE_INTERVAL:
                keepalive = asyncio.create_task(
                    self._keepalive(ws), name=f"{self.name}-keepalive"
                )
            try:
                async for raw in ws:
                    reply = self._handle_frame(raw)
                    if reply is not None:
                        await self._send(ws, reply)
            finally:
                self.connected = False
                if keepalive is not None:
                    keepalive.cancel()

    def _handle_frame(self, raw: str | bytes) -> Any | None:
        """Verarbeitet einen Frame und liefert eine ggf. noetige Antwort zurueck."""
        msg = self.decode_frame(raw)
        self.messages += 1

        reply = self.reactive_reply(msg)
        if reply is not None:
            return reply

        for upd in self.parse(msg):
            self._apply(upd)
        return None

    async def _keepalive(self, ws: Any) -> None:
        assert self.KEEPALIVE_INTERVAL
        while True:
            await asyncio.sleep(self.KEEPALIVE_INTERVAL)
            payload = self.keepalive_payload()
            if payload is not None:
                await self._send(ws, payload)

    # -- REST --------------------------------------------------------------

    async def run_rest(self, session: aiohttp.ClientSession) -> None:
        """Pollt das Orderbook im Sampling-Takt, bis der Task abgebrochen wird."""
        self.transport = "rest"
        self.endpoint = self.REST_BASE
        interval_ms = max(
            self.cfg.rest_interval_ms or self.conn_interval_ms,
            self.MIN_REST_INTERVAL_MS,
        )
        interval = interval_ms / 1000
        self.connected = True
        try:
            while True:
                started = time.monotonic()
                for sym in self.symbols:
                    if sym.listed is False:
                        continue
                    try:
                        upd = await self.rest_depth(session, sym)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.last_error = f"{type(exc).__name__}: {exc}"
                        log.debug("%s REST-Fehler (%s): %s", self.name, sym.native, exc)
                        continue
                    if upd is not None:
                        self.messages += 1
                        self._apply(upd)
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0.0, interval - elapsed))
        finally:
            self.connected = False

    # Wird vom Supervisor gesetzt, damit REST im Sampling-Takt pollt.
    conn_interval_ms: int = 1000

    # -- Buchpflege --------------------------------------------------------

    def _apply(self, upd: BookUpdate) -> None:
        canonical = self.by_native.get(upd.symbol)
        if canonical is None:
            return

        if self.MAINTAINS_BOOK:
            book = self.inc[canonical]
            if upd.is_snapshot:
                book.reset()
            book.apply(upd.bids, upd.asks)
            bids, asks = book.top(self.effective_depth)
        else:
            bids = tuple(upd.bids[: self.effective_depth])
            asks = tuple(upd.asks[: self.effective_depth])

        self.books[canonical] = BookState(
            bids=bids,
            asks=asks,
            ts_recv=now_ms(),
            ts_exchange=upd.ts_exchange,
            seq=upd.seq,
            transport=self.transport,
            endpoint=self.endpoint,
        )
        self.last_update_mono[canonical] = time.monotonic()

    # -- Abgriff durch den Sampler ----------------------------------------

    def snapshot(self, sym: SymbolStatus, ts_grid: int) -> OrderBookSnapshot | None:
        state = self.books.get(sym.canonical)
        if state is None:
            return None

        ts_local = now_ms()
        age_ms = max(0, ts_local - state.ts_recv)

        flags = 0
        if age_ms > self.conn.stale_after_s * 1000:
            if not self.conn.record_stale_snapshots:
                return None
            flags |= FLAG_STALE
        if is_crossed(state.bids, state.asks):
            flags |= FLAG_CROSSED
        if (
            len(state.bids) < self.effective_depth
            or len(state.asks) < self.effective_depth
        ):
            flags |= FLAG_PARTIAL

        return OrderBookSnapshot(
            ts_grid=ts_grid,
            ts_local=ts_local,
            ts_exchange=state.ts_exchange,
            ts_recv=state.ts_recv,
            age_ms=age_ms,
            exchange=self.name,
            symbol=sym.canonical,
            exchange_symbol=sym.native,
            depth=self.effective_depth,
            bids_json=orjson.dumps(state.bids).decode(),
            asks_json=orjson.dumps(state.asks).decode(),
            seq=state.seq,
            transport=state.transport,
            endpoint=state.endpoint,
            flags=flags,
        )

    # -- Symbolpruefung ----------------------------------------------------

    async def validate_symbols(self, session: aiohttp.ClientSession) -> None:
        """Prueft gegen die Instrumentenliste, ob die Paare gelistet sind.

        Ein nicht gelistetes Paar soll laut auffallen, statt still leere Daten
        zu erzeugen - ETH/USDC fuehrt nicht jede der zehn Boersen.
        """
        try:
            listed = await self.fetch_listed_symbols(session)
        except Exception as exc:
            for sym in self.symbols:
                sym.listed = None
                sym.note = f"Pruefung fehlgeschlagen: {type(exc).__name__}"
            log.warning(
                "%s: Instrumentenliste nicht abrufbar (%s) - versuche es trotzdem.",
                self.name,
                exc,
            )
            return

        normalised = {s.upper() for s in listed}
        for sym in self.symbols:
            sym.listed = sym.native.upper() in normalised
            if not sym.listed:
                sym.note = "nicht gelistet"

    def active_symbols(self) -> list[SymbolStatus]:
        return [s for s in self.symbols if s.listed is not False]

    def has_active_symbols(self) -> bool:
        return bool(self.active_symbols())

    # -- Status ------------------------------------------------------------

    def staleness(self) -> float | None:
        """Sekunden seit dem letzten Update ueber alle Symbole (None = noch nie)."""
        if not self.last_update_mono:
            return None
        return time.monotonic() - max(self.last_update_mono.values())
