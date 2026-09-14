#!/usr/bin/env python3
"""Data quality report over the collected snapshots.

    python tools/inspect_db.py [--db data/orderbook.db] [--run latest]

Shows per exchange: row count, gaps in the grid, freshness (age_ms), spread,
share of stale/crossed/partial flags and the connection count. This is the
real acceptance test: it shows not just *that* data is flowing, but whether it
is usable for research.
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

    exchanges = [
        r["exchange"]
        for r in conn.execute(
            "SELECT DISTINCT exchange FROM snapshots WHERE run_id = ? ORDER BY exchange",
            (run_id,),
        )
    ]
    if not exchanges:
        print("No snapshots for this run.")
        return 0

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

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
