"""Common base for all exchange adapters.

Core idea: an adapter keeps a *live* top-N book per symbol in memory. How it
does that - a ready-made top-N push, snapshot plus deltas, or REST polling -
is its own business and invisible to the rest of the program. The sampler
reads that book on the wall-clock grid. As a result an exchange on the REST
fallback produces data of the same shape as one on a WebSocket, and a stalled
connection never blocks the other nine.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import time
import zlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
    TradeEvent,
    is_crossed,
    now_ms,
)

log = logging.getLogger(__name__)


@dataclass(slots=True)
class BookUpdate:
    """A parsed order book frame, in exchange-independent form."""

    symbol: str  # exchange-native spelling
    bids: list[Level]
    asks: list[Level]
    ts_exchange: int | None = None
    seq: int | None = None
    is_snapshot: bool = True
    # The sequence this delta claims to follow. When an exchange supplies it
    # (OKX prevSeqId, Bitget pseq), a mismatch against the last seq we applied
    # proves a message was missed - see _check_sequence.
    prev_seq: int | None = None


class BookSequenceGap(RuntimeError):
    """A delta arrived that does not follow the last one we applied.

    Raised so the supervisor tears the connection down and resubscribes, which
    yields a fresh snapshot. Continuing would mean serving a book that is
    silently wrong from that point on.
    """


@dataclass(slots=True)
class TradeUpdate:
    """One parsed executed trade, in exchange-independent form.

    ``side`` must already be normalised to the **aggressor** (taker) side by the
    adapter, since only the adapter knows its exchange's convention. Keep the
    original value in ``raw_side`` so the normalisation can be audited later.
    """

    symbol: str  # exchange-native spelling
    price: str
    qty: str
    trade_id: str | None = None
    ts_exchange: int | None = None
    side: str | None = None
    raw_side: str | None = None


# The only two values `side` may take once normalised.
BUY, SELL = "buy", "sell"


def taker_from_buyer_maker(buyer_is_maker: Any) -> str:
    """Binance-style flag -> taker side.

    If the buyer was the maker, the seller must have been the aggressor.
    """
    return SELL if bool(buyer_is_maker) else BUY


def invert_side(side: str) -> str:
    return SELL if side == BUY else BUY


def normalise_side(value: Any) -> str | None:
    """'Buy'/'BUY'/'buy' -> 'buy'. Returns None for anything unrecognised."""
    if not isinstance(value, str):
        return None
    low = value.strip().lower()
    if low in ("buy", "b", "bid"):
        return BUY
    if low in ("sell", "s", "ask"):
        return SELL
    return None


@dataclass(slots=True)
class SymbolStatus:
    canonical: str
    native: str
    listed: bool | None = None  # None = not checked
    note: str = ""


@dataclass(slots=True)
class _LastWritten:
    """What was last written for one symbol, for the unchanged-row check."""

    state: BookState  # kept by reference, for the identity fast path
    flags: int
    ts: int


def parse_levels(raw: Any, limit: int | None = None) -> list[Level]:
    """Normalise the various level formats to [(price, quantity), ...].

    Covers ``[["1.0","2.0"], ...]`` (most exchanges),
    ``[["1.0","2.0","0","1"]]`` (OKX/Bitget with extra fields) and
    ``[[1.0, 2.0]]`` (numeric, e.g. HTX). Numbers are converted to strings via
    repr so the write path stays string-based throughout.
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


def sort_levels(levels: list[Level], descending: bool) -> list[Level]:
    """Sort levels by price, best first.

    Most exchanges already deliver bids descending and asks ascending, so this
    is only for the ones that do not: BingX sends its asks worst-price-first.
    Sorting explicitly rather than reversing keeps it correct whichever order
    arrives, and it must happen *before* truncating to the configured depth -
    otherwise the best levels are the ones thrown away.
    """
    return sorted(levels, key=lambda lv: _price(lv[0]), reverse=descending)


def _price(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


class ExchangeAdapter(ABC):
    # -- Per-exchange declaration -----------------------------------------
    name: str = ""
    WS_ENDPOINTS: list[str] = []
    REST_BASE: str = ""
    # Top-N tiers available as a native push. Empty = book is maintained locally.
    PARTIAL_DEPTHS: list[int] = []
    # True when the channel delivers snapshot + deltas that have to be
    # reassembled locally (OKX, Bitget, Bybit, Coinbase).
    MAINTAINS_BOOK: bool = False
    SUPPORTS_TRADES: bool = True
    GZIP_FRAMES: bool = False
    KEEPALIVE_INTERVAL: float | None = None
    SUPPORTS_WS: bool = True
    SUPPORTS_REST: bool = True
    # Fastest REST polling rate, to stay clear of rate limits.
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

        # Set by run.py when stream mode is active. Left as None otherwise, so
        # adapters stay unaware of the writer exactly as before.
        self.on_update: Callable[[OrderBookSnapshot], None] | None = None
        # Set by run.py when trade collection is on.
        self.on_trade: Callable[[TradeEvent], None] | None = None
        self.collect_trades = False
        self.trades_seen = 0
        # Reset on every connect, so "still zero" is evidence the trade
        # subscription itself failed rather than that the market is quiet.
        self.trades_this_connection = 0
        self.seq_gaps = 0
        self._last_seq: dict[str, int] = {}
        self._by_canonical: dict[str, SymbolStatus] = {
            s.canonical: s for s in self.symbols
        }

        # Grid and stream keep separate state: in "both" mode they write to
        # different tables at different moments.
        self._last_grid: dict[str, _LastWritten] = {}
        self._last_stream: dict[str, _LastWritten] = {}
        self.rows_skipped = 0

    # -- To be implemented by subclasses -----------------------------------

    @classmethod
    @abstractmethod
    def native_symbol(cls, canonical: str) -> str:
        """'ETH/USDC' -> the exchange's own spelling."""

    @abstractmethod
    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        """Native symbols the exchange currently trades on spot."""

    @abstractmethod
    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        """A single order book fetched over REST."""

    def parse(self, msg: Any) -> list[BookUpdate]:
        """Frame -> book updates. Empty list for anything not of interest."""
        return []

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        """Frame -> executed trades. Empty list for anything not of interest."""
        return []

    def trade_subscribe_payloads(self) -> list[Any]:
        """Trade-channel subscriptions, sent alongside the book ones.

        Kept separate from :meth:`subscribe_payloads` so the latency probe keeps
        measuring time-to-first-*depth*-message rather than whichever channel
        happens to fire first.
        """
        return []

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        """Recent trades over REST, for the fallback path."""
        return []

    async def ws_url(self, endpoint: str, session: aiohttp.ClientSession) -> str:
        """Lets adapters build the URL dynamically (streams, tokens)."""
        return endpoint

    def subscribe_payloads(self) -> list[Any]:
        """Messages sent immediately after connecting."""
        return []

    def keepalive_payload(self) -> Any | None:
        """Heartbeat to send periodically (interval: KEEPALIVE_INTERVAL)."""
        return None

    def reactive_reply(self, msg: Any) -> Any | None:
        """Reply to a server-initiated ping. None = not a ping."""
        return None

    # -- Depth resolution --------------------------------------------------

    def _resolve_depth(self) -> int:
        """Map the requested depth onto a tier the exchange actually offers."""
        if self.MAINTAINS_BOOK or not self.PARTIAL_DEPTHS:
            # Locally maintained book: any depth is reachable.
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
            "%s: depth %d not available (supported: %s) -> %d [%s]",
            self.name,
            want,
            supported,
            chosen,
            policy,
        )
        return chosen

    # -- Frame handling ----------------------------------------------------

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
                    f"HTTP {resp.status} from {url}: "
                    f"{body[:200].decode('utf-8', 'replace')}"
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
        """Connect, subscribe and run until the connection drops."""
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
            self._last_seq.clear()
            self.trades_this_connection = 0
            self.connected_since = time.monotonic()
            payloads = list(self.subscribe_payloads())
            if self.collect_trades:
                payloads += self.trade_subscribe_payloads()
            for payload in payloads:
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
        """Process one frame and return a reply if the protocol needs one."""
        msg = self.decode_frame(raw)
        self.messages += 1

        reply = self.reactive_reply(msg)
        if reply is not None:
            return reply

        for upd in self.parse(msg):
            self._apply(upd)
        if self.collect_trades:
            for tr in self.parse_trades(msg):
                self._emit_trade(tr)
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
        """Poll the order book on the sampling grid until the task is cancelled."""
        self.transport = "rest"
        self.endpoint = self.REST_BASE
        interval_ms = max(
            self.cfg.rest_interval_ms or self.conn_interval_ms,
            self.MIN_REST_INTERVAL_MS,
        )
        interval = interval_ms / 1000
        self.connected = True
        self.connected_since = time.monotonic()
        self.trades_this_connection = 0
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
                        log.debug("%s REST error (%s): %s", self.name, sym.native, exc)
                        continue
                    if upd is not None:
                        self.messages += 1
                        self._apply(upd)

                    if not self.collect_trades:
                        continue
                    try:
                        for tr in await self.rest_trades(session, sym):
                            self._emit_trade(tr)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.last_error = f"{type(exc).__name__}: {exc}"
                        log.debug(
                            "%s REST trades error (%s): %s", self.name, sym.native, exc
                        )
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0.0, interval - elapsed))
        finally:
            self.connected = False

    # Set by the supervisor so REST polls on the sampling grid.
    conn_interval_ms: int = 1000
    # When the current connection came up; used by the trade watchdog. Defined
    # at class level so it is always readable, even before the first connect.
    connected_since: float = 0.0

    # -- Book maintenance --------------------------------------------------

    def _apply(self, upd: BookUpdate) -> None:
        canonical = self.by_native.get(upd.symbol)
        if canonical is None:
            return

        if self.MAINTAINS_BOOK:
            self._check_sequence(canonical, upd)
            book = self.inc[canonical]
            if upd.is_snapshot:
                book.reset()
            book.apply(upd.bids, upd.asks)
            bids, asks = book.top(self.effective_depth)
        else:
            bids = tuple(upd.bids[: self.effective_depth])
            asks = tuple(upd.asks[: self.effective_depth])

        state = BookState(
            bids=bids,
            asks=asks,
            ts_recv=now_ms(),
            ts_exchange=upd.ts_exchange,
            seq=upd.seq,
            transport=self.transport,
            endpoint=self.endpoint,
        )
        self.books[canonical] = state
        self.last_update_mono[canonical] = time.monotonic()

        if self.on_update is not None:
            self._emit_stream_row(canonical, state, upd.is_snapshot)

    def _emit_stream_row(
        self, canonical: str, state: BookState, is_snapshot: bool
    ) -> None:
        """Stream mode: hand this update straight to the writer."""
        sym = self._by_canonical.get(canonical)
        if sym is None or sym.listed is False:
            return
        flags = self._flags_for(state, state.ts_recv)
        if flags is None:
            return
        if self._is_unchanged(self._last_stream, canonical, state, flags, state.ts_recv):
            return
        self._last_stream[canonical] = _LastWritten(state, flags, state.ts_recv)
        self.on_update(
            self._make_row(
                sym,
                state,
                ts_grid=state.ts_recv,
                ts_local=state.ts_recv,
                flags=flags,
                is_snapshot=is_snapshot,
            )
        )

    def _check_sequence(self, canonical: str, upd: BookUpdate) -> None:
        """Detect a missed delta on a locally maintained book.

        OKX and Bitget chain their updates: each delta names the sequence it
        follows. If that does not match the last one we applied, a message was
        lost and every subsequent level is suspect - the book would keep
        serving quietly wrong data with nothing to indicate it.

        This replaces the checksum both exchanges used to ship: OKX now sends a
        fixed 0 (deprecated 2026-06-23) and Bitget omits the field entirely, so
        sequence chaining is the remaining integrity signal. It is also the
        sharper one, identifying the exact message that went missing.
        """
        if upd.is_snapshot:
            # A snapshot resets the chain; nothing to verify against.
            if upd.seq is not None:
                self._last_seq[canonical] = upd.seq
            return

        last = self._last_seq.get(canonical)
        if upd.prev_seq is not None and last is not None and upd.prev_seq != last:
            self.seq_gaps += 1
            raise BookSequenceGap(
                f"{self.name}/{canonical}: delta follows seq {upd.prev_seq} "
                f"but the last applied was {last} - a message was missed"
            )
        if upd.seq is not None:
            self._last_seq[canonical] = upd.seq

    def _emit_trade(self, tr: TradeUpdate) -> None:
        """Hand one executed trade to the writer.

        No dedupe and no grid: a trade is an event that either happened or did
        not. Repeats are handled in the database instead, by the partial unique
        index on (exchange, symbol, trade_id) - which is what makes the REST
        fallback safe to overlap with the WebSocket.
        """
        if self.on_trade is None:
            return
        canonical = self.by_native.get(tr.symbol)
        if canonical is None:
            return
        sym = self._by_canonical.get(canonical)
        if sym is None or sym.listed is False:
            return
        self.trades_seen += 1
        self.trades_this_connection += 1
        self.on_trade(
            TradeEvent(
                ts_recv=now_ms(),
                ts_exchange=tr.ts_exchange,
                exchange=self.name,
                symbol=canonical,
                exchange_symbol=sym.native,
                trade_id=tr.trade_id,
                price=tr.price,
                qty=tr.qty,
                side=tr.side,
                raw_side=tr.raw_side,
                transport=self.transport,
                endpoint=self.endpoint,
            )
        )

    # -- Recording policy --------------------------------------------------

    def _flags_for(self, state: BookState, ts_local: int) -> int | None:
        """Quality flags for this state, or None if it must not be recorded."""
        flags = 0
        if (ts_local - state.ts_recv) > self.conn.stale_after_s * 1000:
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
        return flags

    def _is_unchanged(
        self,
        tracker: dict[str, _LastWritten],
        canonical: str,
        state: BookState,
        flags: int,
        ts: int,
    ) -> bool:
        """True when this row would duplicate the last one written.

        Compares levels *and* flags: a book that goes stale keeps identical
        levels but must still produce a row, otherwise a dead feed would look
        exactly like a quiet market.
        """
        if not self.skip_unchanged:
            return False
        last = tracker.get(canonical)
        if last is None or last.flags != flags:
            return False

        # Adapters assign a fresh BookState on every update, so an untouched
        # book is still the identical object - settles the common case in O(1)
        # and skips the two orjson.dumps() calls entirely.
        if state is not last.state and (
            state.bids != last.state.bids or state.asks != last.state.asks
        ):
            return False

        if self.heartbeat_ms and (ts - last.ts) >= self.heartbeat_ms:
            return False  # heartbeat due: write it anyway

        self.rows_skipped += 1
        return True

    def _make_row(
        self,
        sym: SymbolStatus,
        state: BookState,
        ts_grid: int,
        ts_local: int,
        flags: int,
        is_snapshot: bool = True,
    ) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            ts_grid=ts_grid,
            ts_local=ts_local,
            ts_exchange=state.ts_exchange,
            ts_recv=state.ts_recv,
            age_ms=max(0, ts_local - state.ts_recv),
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
            is_snapshot=is_snapshot,
        )

    # Recording policy, set from StorageConfig by run.py.
    skip_unchanged: bool = True
    heartbeat_ms: float = 60_000.0

    # -- Read-out by the sampler ------------------------------------------

    def snapshot(self, sym: SymbolStatus, ts_grid: int) -> OrderBookSnapshot | None:
        state = self.books.get(sym.canonical)
        if state is None:
            return None

        ts_local = now_ms()
        flags = self._flags_for(state, ts_local)
        if flags is None:
            return None
        if self._is_unchanged(self._last_grid, sym.canonical, state, flags, ts_local):
            return None

        self._last_grid[sym.canonical] = _LastWritten(state, flags, ts_local)
        return self._make_row(sym, state, ts_grid, ts_local, flags)

    # -- Symbol validation -------------------------------------------------

    async def validate_symbols(self, session: aiohttp.ClientSession) -> None:
        """Check the configured pairs against the exchange's instrument list.

        An unlisted pair should fail loudly instead of silently producing empty
        data - not every exchange lists every pair.
        """
        try:
            listed = await self.fetch_listed_symbols(session)
        except Exception as exc:
            for sym in self.symbols:
                sym.listed = None
                sym.note = f"check failed: {type(exc).__name__}"
            log.warning(
                "%s: instrument list unavailable (%s) - trying anyway.",
                self.name,
                exc,
            )
            return

        normalised = {s.upper() for s in listed}
        for sym in self.symbols:
            sym.listed = sym.native.upper() in normalised
            if not sym.listed:
                sym.note = "not listed"

    def active_symbols(self) -> list[SymbolStatus]:
        return [s for s in self.symbols if s.listed is not False]

    def has_active_symbols(self) -> bool:
        return bool(self.active_symbols())

    # -- Status ------------------------------------------------------------

    def staleness(self) -> float | None:
        """Seconds since the last update across all symbols (None = never)."""
        if not self.last_update_mono:
            return None
        return time.monotonic() - max(self.last_update_mono.values())
