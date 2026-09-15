#!/usr/bin/env python3
"""Data quality report over the collected data.

    python tools/inspect_db.py [--db data/orderbook.db] [--run latest]

Shows per exchange: row count, gaps in the grid, freshness (age_ms), spread,
share of stale/crossed/partial flags and the connection count. This is the
real acceptance test: it shows not just *that* data is flowing, but whether it
is usable for research.

Covers both tables - grid-sampled `snapshots` and, in stream mode,
`book_updates`.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from pathlib import Path

FLAG_STALE = 1 << 0
FLAG_CROSSED = 1 << 1
FLAG_PARTIAL = 1 << 2


def mid_spread(bids_json: str, asks_json: str) -> tuple[float, float] | None:
    bids = json.loads(bids_json)
    asks = json.loads(asks_json)
    if not bids or not asks:
        return None
    try:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
    except (ValueError, IndexError):
        return None
    mid = (best_bid + best_ask) / 2
    spread_bps = (best_ask - best_bid) / mid * 10_000 if mid else 0.0
    return mid, spread_bps


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/orderbook.db")
    parser.add_argument("--run", default="latest", help="'latest' or a run_id")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.is_file():
        print(f"No database found at {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    if args.run == "latest":
        row = conn.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        if row is None:
            print("No runs in the database.")
            return 1
        run_id = row["id"]
    else:
        run_id = int(args.run)

    run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    print(f"Run #{run_id}  started={run['started_at']}  stopped={run['stopped_at']}")

    # A gap only means something went wrong if it outlasts the heartbeat -
    # with skip_unchanged on, ordinary gaps are just deduplicated rows.
    cfg = {}
    try:
        cfg = json.loads(run["config_json"] or "{}")
    except (json.JSONDecodeError, TypeError):
        pass
    interval_ms = cfg.get("general", {}).get("interval_ms", 1000)
    heartbeat_s = cfg.get("storage", {}).get("heartbeat_s", 0)
    skip_unchanged = cfg.get("storage", {}).get("skip_unchanged", False)
    gap_limit_ms = (heartbeat_s * 1000 + 2 * interval_ms) if heartbeat_s else None
    if skip_unchanged:
        print(
            f"skip_unchanged is on; 'Gaps' counts only gaps longer than "
            f"{'%.0f' % (gap_limit_ms / 1000) if gap_limit_ms else '?'}s "
            f"(heartbeat + 2 ticks) - shorter ones are deduplicated rows."
        )

    exchanges = [
        r["exchange"]
        for r in conn.execute(
            "SELECT DISTINCT exchange FROM snapshots WHERE run_id = ? ORDER BY exchange",
            (run_id,),
        )
    ]
    if not exchanges:
        print("No grid snapshots for this run.")
    else:
        _grid_report(conn, run_id, exchanges, gap_limit_ms)

    _stream_report(conn, run_id)
    _skipped_note(conn, run_id)

    conn.close()
    return 0


def _grid_report(
    conn: sqlite3.Connection,
    run_id: int,
    exchanges: list[str],
    gap_limit_ms: float | None,
) -> None:
    print("\n=== Grid samples (snapshots) ===")

    header = (
        f"{'Exchange':10s} {'Rows':>7s} {'Gaps':>8s} {'age_ms avg/max':>16s} "
        f"{'Spread(bps)':>12s} {'stale%':>7s} {'crossed%':>9s} {'partial%':>9s} "
        f"{'connects':>9s}"
    )
    print("\n" + header)
    print("-" * len(header))

    for exchange in exchanges:
        rows = conn.execute(
            "SELECT symbol, ts_grid, age_ms, bids, asks, flags FROM snapshots "
            "WHERE run_id = ? AND exchange = ? ORDER BY symbol, ts_grid",
            (run_id, exchange),
        ).fetchall()
        n = len(rows)
        if n == 0:
            continue

        # Gaps have to be counted per symbol: with several symbols the grid
        # timestamps repeat across them, so a combined series would make the
        # typical step 0 and flag every row as a gap.
        gaps = 0
        by_symbol: dict[str, list[int]] = {}
        for r in rows:
            by_symbol.setdefault(r["symbol"], []).append(r["ts_grid"])
        for grids in by_symbol.values():
            if len(grids) < 2:
                continue
            steps = [b - a for a, b in zip(grids, grids[1:])]
            if gap_limit_ms is not None:
                # Dedupe makes ordinary gaps expected; only a missed heartbeat
                # means the feed actually stopped delivering.
                gaps += sum(1 for s in steps if s > gap_limit_ms)
            else:
                typical = statistics.median(steps)
                if typical > 0:
                    gaps += sum(1 for s in steps if s > typical * 1.5)

        ages = [r["age_ms"] for r in rows if r["age_ms"] is not None]
        age_avg = statistics.mean(ages) if ages else float("nan")
        age_max = max(ages) if ages else float("nan")

        spreads = []
        for r in rows:
            result = mid_spread(r["bids"], r["asks"])
            if result:
                spreads.append(result[1])
        spread_avg = statistics.mean(spreads) if spreads else float("nan")

        stale_pct = 100 * sum(1 for r in rows if r["flags"] & FLAG_STALE) / n
        crossed_pct = 100 * sum(1 for r in rows if r["flags"] & FLAG_CROSSED) / n
        partial_pct = 100 * sum(1 for r in rows if r["flags"] & FLAG_PARTIAL) / n

        # Counts every successful (re)connect, including the very first one -
        # so "1" on an undisturbed run is normal.
        connects = conn.execute(
            "SELECT COUNT(*) c FROM connection_events "
            "WHERE run_id = ? AND exchange = ? AND event = 'connected'",
            (run_id, exchange),
        ).fetchone()["c"]

        print(
            f"{exchange:10s} {n:>7d} {gaps:>8d} "
            f"{age_avg:>7.0f}/{age_max:<7.0f} "
            f"{spread_avg:>12.2f} {stale_pct:>6.1f}% {crossed_pct:>8.1f}% "
            f"{partial_pct:>8.1f}% {connects:>9d}"
        )


def _stream_report(conn: sqlite3.Connection, run_id: int) -> None:
    """Per-exchange summary of the stream table, if it holds anything."""
    try:
        rows = conn.execute(
            "SELECT exchange, COUNT(*) n, MIN(ts_recv) lo, MAX(ts_recv) hi, "
            "       SUM(is_snapshot) snaps, COUNT(DISTINCT symbol) syms "
            "FROM book_updates WHERE run_id = ? GROUP BY exchange ORDER BY exchange",
            (run_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return  # table does not exist (database written by an older version)
    if not rows:
        return

    print("\n=== Stream updates (book_updates) ===")
    header = (
        f"{'Exchange':10s} {'Rows':>8s} {'Symbols':>8s} {'Updates/s':>10s} "
        f"{'Deltas':>8s} {'Snapshots':>10s}"
    )
    print(header)
    print("-" * len(header))
    total = 0
    for r in rows:
        span = max(1.0, (r["hi"] - r["lo"]) / 1000)
        snaps = r["snaps"] or 0
        total += r["n"]
        print(
            f"{r['exchange']:10s} {r['n']:>8d} {r['syms']:>8d} "
            f"{r['n'] / span:>10.1f} {r['n'] - snaps:>8d} {snaps:>10d}"
        )
    print("-" * len(header))
    print(f"{'TOTAL':10s} {total:>8d}")


def _skipped_note(conn: sqlite3.Connection, run_id: int) -> None:
    skipped = conn.execute(
        "SELECT DISTINCT exchange FROM connection_events "
        "WHERE run_id = ? AND event = 'skipped_no_listing'",
        (run_id,),
    ).fetchall()
    if skipped:
        print(
            "\nSkipped (symbol not listed): "
            + ", ".join(r["exchange"] for r in skipped)
        )


if __name__ == "__main__":
    raise SystemExit(main())
