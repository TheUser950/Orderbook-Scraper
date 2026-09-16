#!/usr/bin/env python3
"""Tests for the unchanged-row check, the heartbeat and the stream hook.

Runs without a test framework and without network:  python tests/test_dedupe.py

The whole point of dedupe is that dropping a row must never lose information.
These tests pin down the cases where a row has to be written even though the
levels look identical - above all a feed going stale, which would otherwise be
indistinguishable from a quiet market.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from obscraper.config import ConnectionConfig, ExchangeConfig  # noqa: E402
from obscraper.exchanges.base import BookUpdate  # noqa: E402
from obscraper.exchanges.binance import BinanceAdapter  # noqa: E402
from obscraper.models import FLAG_CROSSED, FLAG_STALE  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


def make_adapter(
    skip_unchanged: bool = True, heartbeat_s: float = 60.0, stale_after_s: float = 15.0
) -> BinanceAdapter:
    cfg = ExchangeConfig(name="binance", depth=2, symbols=["ETH/USDC"])
    adapter = BinanceAdapter(cfg, ConnectionConfig(stale_after_s=stale_after_s))
    adapter.skip_unchanged = skip_unchanged
    adapter.heartbeat_ms = heartbeat_s * 1000
    adapter.symbols[0].listed = True
    return adapter


def feed(adapter: BinanceAdapter, bid: str = "2500.00", ask: str = "2500.10") -> None:
    adapter._apply(
        BookUpdate(
            symbol="ETHUSDC",
            bids=[(bid, "1.0"), ("2499.00", "2.0")],
            asks=[(ask, "1.0"), ("2501.00", "2.0")],
        )
    )


def sym(adapter: BinanceAdapter):
    return adapter.symbols[0]


# -- Grid path ------------------------------------------------------------


def test_unchanged_is_skipped() -> None:
    print("\nGrid: unchanged rows:")
    a = make_adapter()
    feed(a)

    first = a.snapshot(sym(a), 1000)
    check("first sample is written", first is not None)

    second = a.snapshot(sym(a), 1100)
    check("second sample without update is skipped", second is None)
    check("skip counter incremented", a.rows_skipped == 1, str(a.rows_skipped))

    # A new update that produces byte-identical levels must also be skipped -
    # the object differs, so this exercises the slow comparison path.
    feed(a)
    third = a.snapshot(sym(a), 1200)
    check("identical levels from a new update are skipped", third is None)
    check("slow path also counted", a.rows_skipped == 2, str(a.rows_skipped))

    feed(a, bid="2500.50")
    fourth = a.snapshot(sym(a), 1300)
    check("changed levels are written", fourth is not None)


def test_skip_can_be_disabled() -> None:
    print("\nGrid: skip_unchanged = false:")
    a = make_adapter(skip_unchanged=False)
    feed(a)
    check("first written", a.snapshot(sym(a), 1000) is not None)
    check("duplicate written too", a.snapshot(sym(a), 1100) is not None)
    check("nothing counted as skipped", a.rows_skipped == 0, str(a.rows_skipped))


def test_flag_change_forces_a_row() -> None:
    """The important one: a dead feed must not look like a quiet market."""
    print("\nGrid: a flag change always writes:")
    a = make_adapter(stale_after_s=0.05)
    feed(a)
    check("first written", a.snapshot(sym(a), 1000) is not None)
    check("immediate duplicate skipped", a.snapshot(sym(a), 1100) is None)

    # Do not touch the book, just let it age past stale_after_s.
    import time as _time

    _time.sleep(0.12)

    stale_row = a.snapshot(sym(a), 1200)
    check("row written once the book goes stale", stale_row is not None)
    if stale_row is not None:
        check("stale flag set", bool(stale_row.flags & FLAG_STALE), str(stale_row.flags))
    check("still-stale duplicate skipped again", a.snapshot(sym(a), 1300) is None)


def test_crossed_flag_is_detected() -> None:
    print("\nGrid: crossed book:")
    a = make_adapter()
    a._apply(
        BookUpdate(
            symbol="ETHUSDC",
            bids=[("2500.20", "1.0")],
            asks=[("2500.10", "1.0")],  # ask below bid
        )
    )
    row = a.snapshot(sym(a), 1000)
    check("row written", row is not None)
    if row is not None:
        check("crossed flag set", bool(row.flags & FLAG_CROSSED), str(row.flags))


def test_heartbeat() -> None:
    print("\nGrid: heartbeat:")
    a = make_adapter(heartbeat_s=0.05)
    feed(a)
    check("first written", a.snapshot(sym(a), 1000) is not None)
    check("immediate duplicate skipped", a.snapshot(sym(a), 1100) is None)

    import time as _time

    _time.sleep(0.06)
    check("heartbeat writes an unchanged row", a.snapshot(sym(a), 1200) is not None)
    check("and the next one is skipped again", a.snapshot(sym(a), 1300) is None)

    b = make_adapter(heartbeat_s=0)
    feed(b)
    b.snapshot(sym(b), 1000)
    _time.sleep(0.06)
    check("heartbeat_s=0 disables it", b.snapshot(sym(b), 1100) is None)


# -- Stream path ----------------------------------------------------------


def test_stream_hook() -> None:
    print("\nStream: on_update hook:")
    a = make_adapter()
    captured = []
    a.on_update = captured.append

    feed(a)
    check("update is emitted", len(captured) == 1, str(len(captured)))
    if captured:
        row = captured[0]
        check("carries the symbol", row.symbol == "ETH/USDC", row.symbol)
        check("ts_grid is the arrival time", row.ts_grid == row.ts_recv)
        check("marked as a snapshot", row.is_snapshot is True)

    feed(a)  # identical levels
    check("unchanged update is not emitted", len(captured) == 1, str(len(captured)))

    feed(a, ask="2500.99")
    check("changed update is emitted", len(captured) == 2, str(len(captured)))


def test_stream_records_delta_flag() -> None:
    print("\nStream: snapshot vs delta:")
    a = make_adapter()
    captured = []
    a.on_update = captured.append
    a._apply(
        BookUpdate(
            symbol="ETHUSDC",
            bids=[("2500.00", "1.0")],
            asks=[("2500.10", "1.0")],
            is_snapshot=False,
        )
    )
    check("emitted", len(captured) == 1)
    if captured:
        check("is_snapshot=False preserved", captured[0].is_snapshot is False)


def test_grid_and_stream_are_independent() -> None:
    """In 'both' mode the two paths must not consume each other's state."""
    print("\nGrid and stream tracked separately:")
    a = make_adapter()
    captured = []
    a.on_update = captured.append

    feed(a)
    check("stream got the update", len(captured) == 1, str(len(captured)))
    check("grid still writes its first row", a.snapshot(sym(a), 1000) is not None)

    feed(a, bid="2501.00")
    check("stream got the change", len(captured) == 2, str(len(captured)))
    check("grid writes the change too", a.snapshot(sym(a), 1100) is not None)
    check("grid duplicate still skipped", a.snapshot(sym(a), 1200) is None)


def test_unlisted_symbol_is_not_emitted() -> None:
    print("\nStream: unlisted symbols:")
    a = make_adapter()
    a.symbols[0].listed = False
    captured = []
    a.on_update = captured.append
    feed(a)
    check("nothing emitted for an unlisted pair", captured == [], str(len(captured)))


# -- Book integrity -------------------------------------------------------


def test_sequence_gap_detection() -> None:
    """A missed delta must abort the connection, not corrupt the book silently.

    OKX and Bitget chain their updates, each delta naming the sequence it
    follows. This replaced their checksums: OKX now sends a fixed 0 (deprecated
    2026-06-23) and Bitget omits the field entirely.
    """
    print("\nBook sequence continuity:")
    from obscraper.exchanges.base import BookSequenceGap  # noqa: PLC0415
    from obscraper.exchanges.okx import OkxAdapter  # noqa: PLC0415

    def okx_frame(action, seq, prev_seq, px="2500.0"):
        return {
            "arg": {"channel": "books", "instId": "ETH-USDC"},
            "action": action,
            "data": [{
                "bids": [[px, "1", "0", "1"]],
                "asks": [["2501.0", "1", "0", "1"]],
                "ts": "1700000000123", "checksum": 0,
                "seqId": seq, "prevSeqId": prev_seq,
            }],
        }

    a = OkxAdapter(ExchangeConfig(name="okx", depth=20, symbols=["ETH/USDC"]),
                   ConnectionConfig())
    a.symbols[0].listed = True

    for upd in a.parse(okx_frame("snapshot", 100, -1)):
        a._apply(upd)
    check("snapshot accepted", a.books.get("ETH/USDC") is not None)

    for upd in a.parse(okx_frame("update", 101, 100)):
        a._apply(upd)
    check("contiguous delta accepted", a.seq_gaps == 0, str(a.seq_gaps))

    for upd in a.parse(okx_frame("update", 105, 104)):  # 104 != 101
        try:
            a._apply(upd)
            check("gap raises BookSequenceGap", False, "no exception")
        except BookSequenceGap:
            check("gap raises BookSequenceGap", True)
    check("gap counted", a.seq_gaps == 1, str(a.seq_gaps))

    # A fresh snapshot after the reconnect must re-anchor the chain.
    for upd in a.parse(okx_frame("snapshot", 200, -1)):
        a._apply(upd)
    for upd in a.parse(okx_frame("update", 201, 200)):
        a._apply(upd)
    check("snapshot re-anchors the chain", a.seq_gaps == 1, str(a.seq_gaps))

    # books5 sends no sequence fields at all - the check must stay dormant.
    b = OkxAdapter(ExchangeConfig(name="okx", depth=5, symbols=["ETH/USDC"]),
                   ConnectionConfig())
    b.symbols[0].listed = True
    for _ in range(3):
        msg = {"arg": {"channel": "books5", "instId": "ETH-USDC"},
               "data": [{"bids": [["2500.0", "1"]], "asks": [["2501.0", "1"]],
                         "ts": "1700000000123"}]}
        for upd in b.parse(msg):
            b._apply(upd)
    check("no sequence fields -> no false gaps", b.seq_gaps == 0, str(b.seq_gaps))


def test_bitget_sequence_fields() -> None:
    print("\nBitget sequence fields:")
    from obscraper.exchanges.base import BookSequenceGap  # noqa: PLC0415
    from obscraper.exchanges.bitget import BitgetAdapter  # noqa: PLC0415

    def frame(action, seq, pseq):
        return {
            "arg": {"instType": "SPOT", "channel": "books", "instId": "ETHUSDC"},
            "action": action,
            "data": [{"bids": [["2500.0", "1"]], "asks": [["2501.0", "1"]],
                      "ts": "1700000000123", "seq": seq, "pseq": pseq}],
        }

    a = BitgetAdapter(ExchangeConfig(name="bitget", depth=20, symbols=["ETH/USDC"]),
                      ConnectionConfig())
    a.symbols[0].listed = True
    for upd in a.parse(frame("snapshot", 500, 0)):
        a._apply(upd)
    for upd in a.parse(frame("update", 501, 500)):
        a._apply(upd)
    check("contiguous delta accepted", a.seq_gaps == 0, str(a.seq_gaps))
    for upd in a.parse(frame("update", 510, 509)):
        try:
            a._apply(upd)
            check("gap detected", False, "no exception")
        except BookSequenceGap:
            check("gap detected", True)


# -- Regressions ----------------------------------------------------------


def test_bingx_ask_ordering() -> None:
    """BingX sends asks worst-price-first; sort them before truncating.

    Confirmed against the live feed: bids arrive descending as usual, but so do
    asks, which puts the best ask last. Truncating first would keep the worst
    levels and throw the best ones away.
    """
    print("\nRegression: BingX ask ordering:")
    from obscraper.exchanges.bingx import BingXAdapter

    # depth 5 is one of BingX's supported tiers, so effective_depth stays 5;
    # feeding 6 levels per side makes truncation actually bite.
    cfg = ExchangeConfig(name="bingx", depth=5, symbols=["ETH/USDC"])
    a = BingXAdapter(cfg, ConnectionConfig())
    check("effective depth as configured", a.effective_depth == 5, str(a.effective_depth))

    msg = {
        "dataType": "ETH-USDC@depth20",
        "data": {
            "bids": [
                ["2408.13", "1"],
                ["2407.53", "2"],
                ["2407.41", "3"],
                ["2407.00", "4"],
                ["2406.50", "5"],
                ["2385.31", "6"],
            ],
            "asks": [
                ["2429.57", "1"],
                ["2427.16", "2"],
                ["2424.71", "3"],
                ["2420.00", "4"],
                ["2410.00", "5"],
                ["2408.15", "6"],
            ],
        },
    }
    upd = a.parse(msg)[0]
    bid_px = [float(p) for p, _ in upd.bids]
    ask_px = [float(p) for p, _ in upd.asks]
    check("bids descending", bid_px == sorted(bid_px, reverse=True), str(bid_px))
    check("asks ascending", ask_px == sorted(ask_px), str(ask_px))
    check("best ask survives truncation", ask_px[0] == 2408.15, str(ask_px))
    check(
        "truncated to effective depth",
        len(upd.asks) == 5 and len(upd.bids) == 5,
        f"{len(upd.bids)} bids / {len(upd.asks)} asks",
    )
    check(
        "worst ask is the one dropped, not the best",
        2429.57 not in ask_px,
        str(ask_px),
    )
    check(
        "resulting spread is sane",
        round(ask_px[0] - bid_px[0], 6) == 0.02,
        str(ask_px[0] - bid_px[0]),
    )


def test_grid_timestamps_are_absolute() -> None:
    """ts_grid must be a whole multiple of interval_ms.

    Otherwise every run gets its own offset and rows from two runs cannot be
    joined on ts_grid - which is the entire point of grid mode.
    """
    print("\nRegression: grid timestamps:")
    import asyncio as _asyncio

    from obscraper.sampler import Sampler

    for interval in (100, 250, 1000):
        captured: list[int] = []

        class _Collector(Sampler):
            def _sample_once(self, ts_grid: int) -> None:
                captured.append(ts_grid)

        async def drive() -> None:
            s = _Collector([], None, interval)
            stop = _asyncio.Event()
            task = _asyncio.create_task(s.run(stop))
            await _asyncio.sleep(interval / 1000 * 3.5)
            stop.set()
            await task

        _asyncio.run(drive())
        steps = [b - a for a, b in zip(captured, captured[1:])]
        check(
            f"interval={interval}ms: every ts_grid lands on the grid",
            len(captured) >= 2 and all(ts % interval == 0 for ts in captured),
            str(captured[:4]),
        )
        check(
            f"interval={interval}ms: ticks exactly one interval apart",
            bool(steps) and all(s == interval for s in steps),
            str(steps),
        )


if __name__ == "__main__":
    test_unchanged_is_skipped()
    test_skip_can_be_disabled()
    test_flag_change_forces_a_row()
    test_crossed_flag_is_detected()
    test_heartbeat()
    test_stream_hook()
    test_stream_records_delta_flag()
    test_grid_and_stream_are_independent()
    test_unlisted_symbol_is_not_emitted()
    test_sequence_gap_detection()
    test_bitget_sequence_fields()
    test_bingx_ask_ordering()
    test_grid_timestamps_are_absolute()

    print()
    if failures:
        print(f"{len(failures)} test(s) failed: {', '.join(failures)}")
        raise SystemExit(1)
    print("All tests passed.")
