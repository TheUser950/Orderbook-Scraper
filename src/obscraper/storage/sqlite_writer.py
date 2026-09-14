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
from ..models import OrderBookSnapshot, now_ms

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1

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

CREATE UNIQUE INDEX IF NOT EXISTS ux_snap
    ON snapshots(exchange, symbol, ts_grid);
CREATE INDEX IF NOT EXISTS ix_snap_grid ON snapshots(ts_grid);

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

_SENTINEL = object()

SNAP, EVENT, LAT = 0, 1, 2


class SqliteWriter:
    def __init__(self, cfg: StorageConfig) -> None:
        self.cfg = cfg
        self.run_id: int | None = None
        self.written = 0
        self.dropped = 0
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
            "SQLite closed. %d rows written, %d dropped.",
            self.written,
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
        events = [row for kind, row in batch if kind == EVENT]
        lats = [row for kind, row in batch if kind == LAT]

        def _write() -> None:
            conn = self._conn
            if conn is None:
                return
            try:
                if snaps:
                    conn.executemany(_SNAP_SQL, snaps)
                if events:
                    conn.executemany(_EVENT_SQL, events)
                if lats:
                    conn.executemany(_LAT_SQL, lats)
                conn.commit()
            except sqlite3.Error:
                # A failed batch must not terminate the scraper - the running
                # connections are worth more than these few rows.
                conn.rollback()
                raise

        try:
            await asyncio.to_thread(_write)
            self.written += len(snaps)
        except sqlite3.Error as exc:
            self.dropped += len(batch)
            log.error("SQLite write error, %d rows lost: %s", len(batch), exc)
