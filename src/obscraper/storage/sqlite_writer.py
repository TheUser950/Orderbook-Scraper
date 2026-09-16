"""Batched SQLite writer.

A single consumer task drains a bounded queue and writes in batches. The
actual SQLite calls run via ``asyncio.to_thread`` so an fsync does not stall
the event loop and thereby shift the sampling grid.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path

from ..config import StorageConfig
from ..models import OrderBookSnapshot, TradeEvent, now_ms

log = logging.getLogger(__name__)

SCHEMA_VERSION = 3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY,
    started_at  INTEGER NOT NULL,
    stopped_at  INTEGER,
    version     TEXT,
    config_hash TEXT,
    config_json TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY,
    run_id          INTEGER,
    ts_grid         INTEGER NOT NULL,
    ts_local        INTEGER NOT NULL,
    ts_exchange     INTEGER,
    ts_recv         INTEGER,
    age_ms          INTEGER,
    exchange        TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    exchange_symbol TEXT    NOT NULL,
    depth           INTEGER NOT NULL,
    bids            TEXT    NOT NULL,
    asks            TEXT    NOT NULL,
    seq             INTEGER,
    transport       TEXT    NOT NULL,
    endpoint        TEXT,
    flags           INTEGER NOT NULL DEFAULT 0
);

-- run_id is part of the key: two runs restarted within the same grid tick are
-- distinct observations, and without it the second would be silently dropped
-- by INSERT OR IGNORE. Schema v1 had it without run_id; dropping that is safe
-- because the new key is a superset of the old one.
DROP INDEX IF EXISTS ux_snap;
CREATE UNIQUE INDEX IF NOT EXISTS ux_snap_run
    ON snapshots(run_id, exchange, symbol, ts_grid);
CREATE INDEX IF NOT EXISTS ix_snap_grid ON snapshots(ts_grid);

-- Stream mode. Deliberately not part of snapshots: this is an event log, not
-- a grid, and two updates for one symbol can land in the same millisecond -
-- a unique index on time would silently drop them.
CREATE TABLE IF NOT EXISTS book_updates (
    id              INTEGER PRIMARY KEY,
    run_id          INTEGER,
    ts_recv         INTEGER NOT NULL,
    ts_exchange     INTEGER,
    exchange        TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    exchange_symbol TEXT    NOT NULL,
    depth           INTEGER NOT NULL,
    bids            TEXT    NOT NULL,
    asks            TEXT    NOT NULL,
    seq             INTEGER,
    transport       TEXT    NOT NULL,
    endpoint        TEXT,
    flags           INTEGER NOT NULL DEFAULT 0,
    is_snapshot     INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_upd ON book_updates(exchange, symbol, ts_recv);

-- Executed trades. Events, not state: there is no grid concept here, every
-- trade is written as it arrives.
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY,
    run_id          INTEGER,
    ts_recv         INTEGER NOT NULL,
    ts_exchange     INTEGER,
    exchange        TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    exchange_symbol TEXT    NOT NULL,
    trade_id        TEXT,
    price           TEXT    NOT NULL,
    qty             TEXT    NOT NULL,
    side            TEXT,
    raw_side        TEXT,
    transport       TEXT    NOT NULL,
    endpoint        TEXT
);

-- Partial index: deduplicates where the exchange supplies an id, and never
-- drops rows where it does not (MEXC sends no trade id at all). Makes the REST
-- fallback safe, since overlapping polls re-deliver trades already stored.
-- run_id is deliberately absent: the same trade seen by two runs is one trade.
CREATE UNIQUE INDEX IF NOT EXISTS ux_trade
    ON trades(exchange, symbol, trade_id) WHERE trade_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_trade_ts ON trades(exchange, symbol, ts_exchange);

CREATE TABLE IF NOT EXISTS connection_events (
    id       INTEGER PRIMARY KEY,
    ts       INTEGER NOT NULL,
    run_id   INTEGER,
    exchange TEXT    NOT NULL,
    event    TEXT    NOT NULL,
    endpoint TEXT,
    detail   TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON connection_events(ts);

CREATE TABLE IF NOT EXISTS endpoint_latency (
    id           INTEGER PRIMARY KEY,
    ts           INTEGER NOT NULL,
    run_id       INTEGER,
    exchange     TEXT    NOT NULL,
    endpoint     TEXT    NOT NULL,
    handshake_ms REAL,
    first_msg_ms REAL,
    chosen       INTEGER NOT NULL DEFAULT 0
);
"""

_SNAP_SQL = """
INSERT OR IGNORE INTO snapshots
    (run_id, ts_grid, ts_local, ts_exchange, ts_recv, age_ms, exchange, symbol,
     exchange_symbol, depth, bids, asks, seq, transport, endpoint, flags)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

_EVENT_SQL = """
INSERT INTO connection_events (ts, run_id, exchange, event, endpoint, detail)
VALUES (?,?,?,?,?,?)
"""

_LAT_SQL = """
INSERT INTO endpoint_latency
    (ts, run_id, exchange, endpoint, handshake_ms, first_msg_ms, chosen)
VALUES (?,?,?,?,?,?,?)
"""

_UPD_SQL = """
INSERT INTO book_updates
    (run_id, ts_recv, ts_exchange, exchange, symbol, exchange_symbol, depth,
     bids, asks, seq, transport, endpoint, flags, is_snapshot)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

_TRADE_SQL = """
INSERT OR IGNORE INTO trades
    (run_id, ts_recv, ts_exchange, exchange, symbol, exchange_symbol, trade_id,
     price, qty, side, raw_side, transport, endpoint)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

_SENTINEL = object()

SNAP, EVENT, LAT, UPD, TRD = 0, 1, 2, 3, 4


class SqliteWriter:
    def __init__(self, cfg: StorageConfig) -> None:
        self.cfg = cfg
        self.run_id: int | None = None
        self.written = 0
        self.dropped = 0
        # Submitted but skipped as an existing row (INSERT OR IGNORE).
        self.ignored = 0
        self._conn: sqlite3.Connection | None = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_maxsize)
        self._task: asyncio.Task | None = None
        self._warned_full = False

    # -- Lifecycle ---------------------------------------------------------

    async def start(self, config_json: str, config_hash: str, version: str) -> int:
        path = Path(self.cfg.path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)

        def _open() -> int:
            conn = sqlite3.connect(str(path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            cur = conn.execute(
                "INSERT INTO runs (started_at, version, config_hash, config_json) "
                "VALUES (?,?,?,?)",
                (now_ms(), version, config_hash, config_json),
            )
            conn.commit()
            self._conn = conn
            return int(cur.lastrowid)

        self.run_id = await asyncio.to_thread(_open)
        self._task = asyncio.create_task(self._drain_loop(), name="sqlite-writer")
        log.info("SQLite ready: %s (run_id=%s)", path, self.run_id)
        return self.run_id

    async def close(self) -> None:
        if self._task is None:
            return
        await self._queue.put(_SENTINEL)
        try:
            await asyncio.wait_for(self._task, timeout=30)
        except TimeoutError:
            log.error("Writer task did not drain in time during shutdown.")
            self._task.cancel()

        def _finish() -> None:
            if self._conn is None:
                return
            self._conn.execute(
                "UPDATE runs SET stopped_at = ? WHERE id = ?", (now_ms(), self.run_id)
            )
            self._conn.commit()
            self._conn.close()

        await asyncio.to_thread(_finish)
        self._conn = None
        log.info(
            "SQLite closed. %d rows stored, %d already present, %d dropped.",
            self.written,
            self.ignored,
            self.dropped,
        )

    # -- Intake ------------------------------------------------------------

    def submit_snapshot(self, snap: OrderBookSnapshot) -> None:
        self._put(
            (
                SNAP,
                (
                    self.run_id,
                    snap.ts_grid,
                    snap.ts_local,
                    snap.ts_exchange,
                    snap.ts_recv,
                    snap.age_ms,
                    snap.exchange,
                    snap.symbol,
                    snap.exchange_symbol,
                    snap.depth,
                    snap.bids_json,
                    snap.asks_json,
                    snap.seq,
                    snap.transport,
                    snap.endpoint,
                    snap.flags,
                ),
            )
        )

    def submit_update(self, snap: OrderBookSnapshot) -> None:
        self._put(
            (
                UPD,
                (
                    self.run_id,
                    snap.ts_recv,
                    snap.ts_exchange,
                    snap.exchange,
                    snap.symbol,
                    snap.exchange_symbol,
                    snap.depth,
                    snap.bids_json,
                    snap.asks_json,
                    snap.seq,
                    snap.transport,
                    snap.endpoint,
                    snap.flags,
                    int(snap.is_snapshot),
                ),
            )
        )

    def submit_trade(self, tr: TradeEvent) -> None:
        self._put(
            (
                TRD,
                (
                    self.run_id,
                    tr.ts_recv,
                    tr.ts_exchange,
                    tr.exchange,
                    tr.symbol,
                    tr.exchange_symbol,
                    tr.trade_id,
                    tr.price,
                    tr.qty,
                    tr.side,
                    tr.raw_side,
                    tr.transport,
                    tr.endpoint,
                ),
            )
        )

    def submit_event(
        self,
        exchange: str,
        event: str,
        endpoint: str | None = None,
        detail: str | None = None,
    ) -> None:
        self._put((EVENT, (now_ms(), self.run_id, exchange, event, endpoint, detail)))

    def submit_latency(
        self,
        exchange: str,
        endpoint: str,
        handshake_ms: float | None,
        first_msg_ms: float | None,
        chosen: bool,
    ) -> None:
        self._put(
            (
                LAT,
                (
                    now_ms(),
                    self.run_id,
                    exchange,
                    endpoint,
                    handshake_ms,
                    first_msg_ms,
                    int(chosen),
                ),
            )
        )

    def _put(self, item: tuple) -> None:
        try:
            self._queue.put_nowait(item)
            return
        except asyncio.QueueFull:
            pass
        # The queue is bounded so a stalled writer cannot silently eat memory.
        # When in doubt, the oldest snapshot is the least interesting one.
        try:
            self._queue.get_nowait()
            self.dropped += 1
        except asyncio.QueueEmpty:
            pass
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self.dropped += 1
        if not self._warned_full:
            self._warned_full = True
            log.warning(
                "Write queue is full (maxsize=%d) - rows are being dropped. "
                "Interval too small or storage too slow?",
                self.cfg.queue_maxsize,
            )

    # -- Consumer ----------------------------------------------------------

    async def _drain_loop(self) -> None:
        flush_interval = self.cfg.flush_interval_ms / 1000
        batch: list[tuple] = []
        last_flush = time.monotonic()

        while True:
            timeout = max(0.01, flush_interval - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            except TimeoutError:
                item = None

            if item is _SENTINEL:
                while not self._queue.empty():
                    rest = self._queue.get_nowait()
                    if rest is not _SENTINEL:
                        batch.append(rest)
                await self._flush(batch)
                return

            if item is not None:
                batch.append(item)

            due = time.monotonic() - last_flush >= flush_interval
            if len(batch) >= self.cfg.batch_size or (batch and due):
                await self._flush(batch)
                batch = []
                last_flush = time.monotonic()

    async def _flush(self, batch: list[tuple]) -> None:
        if not batch or self._conn is None:
            return
        snaps = [row for kind, row in batch if kind == SNAP]
        updates = [row for kind, row in batch if kind == UPD]
        trades = [row for kind, row in batch if kind == TRD]
        events = [row for kind, row in batch if kind == EVENT]
        lats = [row for kind, row in batch if kind == LAT]

        def _write() -> int:
            conn = self._conn
            if conn is None:
                return 0
            # Count rows that actually landed. Both snapshots and trades use
            # INSERT OR IGNORE, so a submitted row may be silently skipped as a
            # duplicate - and during REST polling, where windows overlap
            # heavily, most of them are. Reporting submissions would overstate
            # the stored data several-fold.
            before = conn.total_changes
            try:
                if snaps:
                    conn.executemany(_SNAP_SQL, snaps)
                if updates:
                    conn.executemany(_UPD_SQL, updates)
                if trades:
                    conn.executemany(_TRADE_SQL, trades)
                if events:
                    conn.executemany(_EVENT_SQL, events)
                if lats:
                    conn.executemany(_LAT_SQL, lats)
                conn.commit()
                return conn.total_changes - before
            except sqlite3.Error:
                # A failed batch must not terminate the scraper - the running
                # connections are worth more than these few rows.
                conn.rollback()
                raise

        try:
            inserted = await asyncio.to_thread(_write)
            self.written += inserted
            self.ignored += (
                len(snaps) + len(updates) + len(trades) + len(events) + len(lats)
            ) - inserted
        except sqlite3.Error as exc:
            self.dropped += len(batch)
            log.error("SQLite write error, %d rows lost: %s", len(batch), exc)
