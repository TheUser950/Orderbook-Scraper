#!/usr/bin/env python3
"""Tests for trade parsing, above all the aggressor-side normalisation.

Runs without a test framework and without network:  python tests/test_trades.py

`side` is always the **taker** (aggressor). Exchanges disagree on how they
express that and two of them report the maker side instead, so getting it wrong
produces data that looks completely plausible while inverting every order-flow
conclusion drawn from that exchange. These tests pin the convention down with a
captured frame per exchange.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from obscraper.config import ConnectionConfig, ExchangeConfig  # noqa: E402
from obscraper.exchanges.registry import REGISTRY, create_adapter  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


def adapter(name: str, symbol: str = "ETH/USDC", depth: int = 20):
    cfg = ExchangeConfig(name=name, depth=depth, symbols=[symbol])
    a = create_adapter(name, cfg, ConnectionConfig())
    a.collect_trades = True
    a.symbols[0].listed = True
    return a


def one(a, msg):
    """Parse a frame and return the single trade it should contain."""
    trades = a.parse_trades(msg)
    assert len(trades) == 1, f"expected 1 trade, got {len(trades)}"
    return trades[0]


# -- Side normalisation, one captured frame shape per exchange ------------


def test_binance() -> None:
    print("\nBinance (m = buyer is maker -> invert):")
    a = adapter("binance")
    base = {"s": "ETHUSDC", "p": "2500.10", "q": "1.5", "t": 12345, "T": 1700000000123}
    sell = one(a, {"stream": "ethusdc@trade", "data": {**base, "m": True}})
    buy = one(a, {"stream": "ethusdc@trade", "data": {**base, "m": False}})
    check("m=True  -> taker sell", sell.side == "sell", sell.side)
    check("m=False -> taker buy", buy.side == "buy", buy.side)
    check("price preserved exactly", sell.price == "2500.10", sell.price)
    check("qty preserved exactly", sell.qty == "1.5", sell.qty)
    check("trade id captured", sell.trade_id == "12345", str(sell.trade_id))
    check("exchange timestamp", sell.ts_exchange == 1700000000123)
    check("raw side retained", "m=True" in (sell.raw_side or ""), str(sell.raw_side))
    check("depth frame yields no trades", a.parse_trades(
        {"stream": "ethusdc@depth20@100ms", "data": {"bids": [], "asks": []}}) == [])


def test_coinbase() -> None:
    """The important one: Coinbase reports the MAKER side."""
    print("\nCoinbase (side = maker -> invert):")
    a = adapter("coinbase", symbol="ETH/USDT")
    base = {
        "type": "match",
        "product_id": "ETH-USDT",
        "price": "2500.10",
        "size": "0.5",
        "trade_id": 777,
        "time": "2026-09-16T20:00:00.123456Z",
    }
    maker_sell = one(a, {**base, "side": "sell"})
    maker_buy = one(a, {**base, "side": "buy"})
    check("maker sell -> taker BUY", maker_sell.side == "buy", maker_sell.side)
    check("maker buy  -> taker SELL", maker_buy.side == "sell", maker_buy.side)
    check(
        "raw side records it was the maker",
        "maker=sell" in (maker_sell.raw_side or ""),
        str(maker_sell.raw_side),
    )
    # 2026-09-16T20:00:00.123456Z in epoch ms, interpreted as UTC (the "Z" must
    # not be dropped, or the value would shift by the local timezone offset).
    check("ISO timestamp converted to ms",
          maker_sell.ts_exchange == 1789588800123, str(maker_sell.ts_exchange))
    check("last_match replay also parsed",
          len(a.parse_trades({**base, "type": "last_match", "side": "buy"})) == 1)


def test_taker_side_exchanges() -> None:
    """The six exchanges that already report the taker side directly."""
    print("\nExchanges reporting the taker side directly:")

    cases = [
        ("okx", {"arg": {"channel": "trades", "instId": "ETH-USDC"},
                 "data": [{"px": "2500.1", "sz": "1", "side": "buy",
                           "tradeId": "9", "ts": "1700000000123"}]}),
        ("bybit", {"topic": "publicTrade.ETHUSDC",
                   "data": [{"s": "ETHUSDC", "p": "2500.1", "v": "1", "S": "Buy",
                             "i": "9", "T": 1700000000123}]}),
        ("bitget", {"arg": {"channel": "trade", "instId": "ETHUSDC"},
                    "data": [{"price": "2500.1", "size": "1", "side": "buy",
                              "tradeId": "9", "ts": "1700000000123"}]}),
        ("gate", {"channel": "spot.trades", "event": "update",
                  "result": {"currency_pair": "ETH_USDC", "price": "2500.1",
                             "amount": "1", "side": "buy", "id": 9,
                             "create_time_ms": "1700000000123.456"}}),
        ("htx", {"ch": "market.ethusdc.trade.detail",
                 "tick": {"data": [{"price": 2500.1, "amount": 1,
                                    "direction": "buy", "tradeId": 9,
                                    "ts": 1700000000123}]}}),
        ("kucoin", {"type": "message", "topic": "/market/match:ETH-USDC",
                    "data": {"price": "2500.1", "size": "1", "side": "buy",
                             "tradeId": "9", "time": "1700000000123000000"}}),
    ]

    for name, msg in cases:
        a = adapter(name)
        tr = one(a, msg)
        check(f"{name}: side passes through as taker buy", tr.side == "buy", str(tr.side))
        check(f"{name}: trade id captured", tr.trade_id == "9", str(tr.trade_id))
        check(
            f"{name}: timestamp normalised to ms",
            tr.ts_exchange == 1700000000123,
            str(tr.ts_exchange),
        )

    # ... and the sell direction, to prove nothing is hardcoded.
    a = adapter("okx")
    sell = one(a, {"arg": {"channel": "trades", "instId": "ETH-USDC"},
                   "data": [{"px": "2500.1", "sz": "1", "side": "sell",
                             "tradeId": "10", "ts": "1700000000123"}]})
    check("okx: sell stays sell", sell.side == "sell", str(sell.side))


def test_mexc_protobuf_trades() -> None:
    print("\nMEXC (protobuf, tradeType 1=buy 2=sell):")
    from tests_helpers import build_mexc_deals_frame  # noqa: PLC0415

    a = adapter("mexc")
    buy = one(a, ("pb", build_mexc_deals_frame(trade_type=1)))
    sell = one(a, ("pb", build_mexc_deals_frame(trade_type=2)))
    check("tradeType=1 -> buy", buy.side == "buy", str(buy.side))
    check("tradeType=2 -> sell", sell.side == "sell", str(sell.side))
    check("price exact", buy.price == "2500.10", buy.price)
    check("qty exact", buy.qty == "1.5", buy.qty)
    check("timestamp", buy.ts_exchange == 1700000000123, str(buy.ts_exchange))
    check("trade id read from the aggregated item", buy.trade_id == "T-42",
          str(buy.trade_id))
    check("raw side recorded", "tradeType=1" in (buy.raw_side or ""), str(buy.raw_side))

    # MEXC blocks the plain channel, but the parser must still cope with it -
    # there it is wrapper field 301 and the item carries no trade id.
    plain = one(a, ("pb", build_mexc_deals_frame(trade_type=2, aggregated=False)))
    check("plain deals frame still parses", plain.side == "sell", str(plain.side))
    check("plain deals frame has no trade id", plain.trade_id is None,
          str(plain.trade_id))

    # A depth frame must not be mistaken for trades.
    from tests_helpers import field_msg, field_str, field_varint  # noqa: PLC0415

    depth_frame = (
        field_str(1, "spot@public.limit.depth.v3.api.pb@ETHUSDC@20")
        + field_msg(303, field_str(4, "1"))
        + field_str(3, "ETHUSDC")
    )
    check("depth frame yields no trades", a.parse_trades(("pb", depth_frame)) == [])


def test_bingx() -> None:
    """BingX's side is deliberately unknown - see the note in bingx.py.

    Its `m` flag failed validation on two independent tests (trade-vs-book and
    a book-independent tick test, the latter at 47.8% = random), so we record
    the flag but refuse to claim a direction. This test locks that decision in
    so nobody 'fixes' it back to a guess.
    """
    print("\nBingX (side deliberately unknown):")
    a = adapter("bingx")
    base = {"dataType": "ETH-USDC@trade"}
    t_true = one(a, {**base, "data": {"p": "2500.1", "q": "1", "t": 9,
                                      "T": 1700000000123, "m": True}})
    t_false = one(a, {**base, "data": {"p": "2500.1", "q": "1", "t": 9,
                                       "T": 1700000000123, "m": False}})
    check("side is not asserted for m=True", t_true.side is None, str(t_true.side))
    check("side is not asserted for m=False", t_false.side is None, str(t_false.side))
    check("raw flag preserved (m=True)", t_true.raw_side == "m=True", str(t_true.raw_side))
    check("raw flag preserved (m=False)", t_false.raw_side == "m=False",
          str(t_false.raw_side))
    check("price/qty/id still captured",
          (t_true.price, t_true.qty, t_true.trade_id) == ("2500.1", "1", "9"),
          f"{t_true.price},{t_true.qty},{t_true.trade_id}")
    check("depth frame yields no trades", a.parse_trades(
        {"dataType": "ETH-USDC@depth20", "data": {"bids": [], "asks": []}}) == [])


# -- Plumbing -------------------------------------------------------------


def test_every_exchange_subscribes() -> None:
    print("\nAll ten exchanges subscribe to trades:")
    for name in REGISTRY:
        a = adapter(name)
        if name == "binance":
            # Binance selects streams through the URL, not a subscribe message.
            url = asyncio.run(a.ws_url("wss://x", None))
            check("binance: @trade present in the stream URL", "@trade" in url, url[-40:])
            a.collect_trades = False
            url_off = asyncio.run(a.ws_url("wss://x", None))
            check("binance: absent when trades are off", "@trade" not in url_off)
            continue
        check(f"{name}: has a trade subscription", len(a.trade_subscribe_payloads()) >= 1)


def test_emit_requires_listing() -> None:
    print("\nEmission rules:")
    a = adapter("okx")
    captured = []
    a.on_trade = captured.append
    msg = {"arg": {"channel": "trades", "instId": "ETH-USDC"},
           "data": [{"px": "2500.1", "sz": "1", "side": "buy", "tradeId": "9",
                     "ts": "1700000000123"}]}

    a._handle_frame_trades = None  # not used; emit through the real path
    for tr in a.parse_trades(msg):
        a._emit_trade(tr)
    check("listed symbol is emitted", len(captured) == 1, str(len(captured)))
    if captured:
        ev = captured[0]
        check("canonical symbol on the event", ev.symbol == "ETH/USDC", ev.symbol)
        check("native symbol retained", ev.exchange_symbol == "ETH-USDC", ev.exchange_symbol)
        check("exchange name set", ev.exchange == "okx", ev.exchange)
        check("counter incremented", a.trades_seen == 1, str(a.trades_seen))

    b = adapter("okx")
    b.symbols[0].listed = False
    got = []
    b.on_trade = got.append
    for tr in b.parse_trades(msg):
        b._emit_trade(tr)
    check("unlisted symbol is not emitted", got == [], str(len(got)))


def test_trades_off_means_silent() -> None:
    print("\nTrades disabled:")
    a = adapter("okx")
    a.collect_trades = False
    captured = []
    a.on_trade = captured.append
    a._handle_frame(
        '{"arg":{"channel":"trades","instId":"ETH-USDC"},'
        '"data":[{"px":"2500.1","sz":"1","side":"buy","tradeId":"9","ts":"1"}]}'
    )
    check("no trades emitted when collect_trades is off", captured == [])


if __name__ == "__main__":
    test_binance()
    test_coinbase()
    test_taker_side_exchanges()
    test_mexc_protobuf_trades()
    test_bingx()
    test_every_exchange_subscribes()
    test_emit_requires_listing()
    test_trades_off_means_silent()

    print()
    if failures:
        print(f"{len(failures)} test(s) failed: {', '.join(failures)}")
        raise SystemExit(1)
    print("All tests passed.")
