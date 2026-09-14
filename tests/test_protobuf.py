#!/usr/bin/env python3
"""Tests for the protobuf wire-format reader and the MEXC parsing.

Runs without a test framework:  python tests/test_protobuf.py

Hand-rolled wire-format decoding is easy to get subtly wrong, so this checks
against self-encoded frames - including the cases that actually matter in
production: unknown fields (additive schema change) and truncated frames.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from obscraper.config import ConnectionConfig, ExchangeConfig  # noqa: E402
from obscraper.exchanges._protobuf import (  # noqa: E402
    ProtobufError,
    get_int,
    get_str,
    get_submessages,
    parse_message,
)
from obscraper.exchanges.mexc import MexcAdapter  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


# -- Minimal protobuf encoder, for the tests only -------------------------


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def tag(field_no: int, wire: int) -> bytes:
    return varint((field_no << 3) | wire)


def field_str(field_no: int, text: str) -> bytes:
    raw = text.encode()
    return tag(field_no, 2) + varint(len(raw)) + raw


def field_msg(field_no: int, body: bytes) -> bytes:
    return tag(field_no, 2) + varint(len(body)) + body


def field_varint(field_no: int, value: int) -> bytes:
    return tag(field_no, 0) + varint(value)


def level_item(price: str, qty: str) -> bytes:
    return field_str(1, price) + field_str(2, qty)


def build_mexc_frame(
    symbol: str = "ETHUSDC",
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    send_time: int = 1700000000123,
    version: str = "987654",
    extra_unknown: bytes = b"",
) -> bytes:
    """Build a frame the way MEXC sends one for limit.depth."""
    bids = bids if bids is not None else [("2500.10", "1.5"), ("2500.00", "3.0")]
    asks = asks if asks is not None else [("2500.20", "2.0"), ("2500.30", "4.0")]

    depths = b"".join(field_msg(1, level_item(p, q)) for p, q in asks)
    depths += b"".join(field_msg(2, level_item(p, q)) for p, q in bids)
    depths += field_str(3, "spot@public.limit.depth.v3.api.pb")
    depths += field_str(4, version)

    wrapper = field_str(1, f"spot@public.limit.depth.v3.api.pb@{symbol}@20")
    wrapper += field_msg(303, depths)
    wrapper += field_str(3, symbol)
    wrapper += field_varint(6, send_time)
    wrapper += extra_unknown
    return wrapper


# -- Tests ----------------------------------------------------------------


def test_wire_format() -> None:
    print("\nWire format:")
    msg = parse_message(field_str(1, "hello") + field_varint(2, 300))
    check("string field", get_str(msg, 1) == "hello", repr(get_str(msg, 1)))
    check("varint field (multi-byte)", get_int(msg, 2) == 300, repr(get_int(msg, 2)))
    check("missing field -> None", get_str(msg, 99) is None)

    # Repeated fields must keep their order - otherwise the level ordering of
    # the order book would be broken.
    repeated = b"".join(field_msg(1, level_item(p, "1")) for p in ["3", "2", "1"])
    subs = get_submessages(parse_message(repeated), 1)
    order = [get_str(s, 1) for s in subs]
    check("order of repeated fields", order == ["3", "2", "1"], repr(order))

    for name, data in [
        ("truncated varint", b"\x08\x80"),
        ("length past end of frame", b"\x0a\xff\x01ab"),
        ("field number 0", b"\x00\x01"),
    ]:
        try:
            parse_message(data)
            check(f"{name} raises ProtobufError", False, "no exception")
        except ProtobufError:
            check(f"{name} raises ProtobufError", True)


def make_adapter(depth: int = 20) -> MexcAdapter:
    cfg = ExchangeConfig(name="mexc", depth=depth, symbols=["ETH/USDC"])
    return MexcAdapter(cfg, ConnectionConfig())


def test_mexc_parse() -> None:
    print("\nMEXC frame:")
    adapter = make_adapter()
    updates = adapter.parse(("pb", build_mexc_frame()))
    check("exactly one update", len(updates) == 1, f"{len(updates)}")
    if not updates:
        return
    upd = updates[0]
    check("symbol", upd.symbol == "ETHUSDC", upd.symbol)
    check("bids", upd.bids == [("2500.10", "1.5"), ("2500.00", "3.0")], str(upd.bids))
    check("asks", upd.asks == [("2500.20", "2.0"), ("2500.30", "4.0")], str(upd.asks))
    check("exchange timestamp", upd.ts_exchange == 1700000000123, str(upd.ts_exchange))
    check("sequence from version", upd.seq == 987654, str(upd.seq))
    check("is a snapshot", upd.is_snapshot is True)

    # Prices must be passed through exactly as strings - a float conversion
    # would be lossy on the write path.
    exact = build_mexc_frame(bids=[("0.000000012345678", "9999999.123456789")])
    upd2 = adapter.parse(("pb", exact))[0]
    check(
        "price string unchanged",
        upd2.bids[0] == ("0.000000012345678", "9999999.123456789"),
        str(upd2.bids[0]),
    )


def test_schema_change_tolerance() -> None:
    """The actual point: additive schema changes must not disturb anything."""
    print("\nTolerance towards schema changes:")
    adapter = make_adapter()

    unknown = (
        field_str(42, "a new field")
        + field_varint(43, 12345)
        + field_msg(44, field_str(1, "nested new"))
    )
    updates = adapter.parse(("pb", build_mexc_frame(extra_unknown=unknown)))
    check("unknown fields are skipped", len(updates) == 1, str(len(updates)))
    if updates:
        check(
            "data correct despite unknown fields",
            updates[0].bids == [("2500.10", "1.5"), ("2500.00", "3.0")],
            str(updates[0].bids),
        )

    # Broken frames may only be counted, not raised - otherwise a single frame
    # would tear down the whole connection.
    before = adapter.protobuf_errors
    result = adapter.parse(("pb", b"\x0a\xff\xff\xff\x7f"))
    check("broken frame does not raise", result == [], str(result))
    check("error counter incremented", adapter.protobuf_errors == before + 1)

    # A wrapper without a depth body (different channel) is not an error.
    other = field_str(1, "spot@public.deals.v3.api.pb@ETHUSDC") + field_str(3, "ETHUSDC")
    errors_before = adapter.protobuf_errors
    check("foreign channel -> empty", adapter.parse(("pb", other)) == [])
    check("foreign channel is not an error", adapter.protobuf_errors == errors_before)


def test_depth_truncation() -> None:
    print("\nDepth truncation:")
    adapter = make_adapter(depth=5)
    many = [(f"{2500 - i}", "1.0") for i in range(20)]
    upd = adapter.parse(("pb", build_mexc_frame(bids=many, asks=many)))[0]
    check("bids truncated to effective_depth", len(upd.bids) == 5, str(len(upd.bids)))
    check("asks truncated to effective_depth", len(upd.asks) == 5, str(len(upd.asks)))


def test_control_frames() -> None:
    print("\nControl messages:")
    adapter = make_adapter()
    check("JSON ack yields no updates", adapter.parse({"id": 0, "code": 0}) == [])
    check(
        "server PING is answered",
        adapter.reactive_reply({"id": 0, "code": 0, "msg": "PING"})
        == {"method": "PONG"},
    )
    check("normal message needs no reply", adapter.reactive_reply({}) is None)
    check(
        "binary frame is tagged as protobuf",
        adapter.decode_frame(b"\x08\x01") == ("pb", b"\x08\x01"),
    )
    check(
        "text frame is read as JSON",
        adapter.decode_frame('{"code":0}') == {"code": 0},
    )


if __name__ == "__main__":
    test_wire_format()
    test_mexc_parse()
    test_schema_change_tolerance()
    test_depth_truncation()
    test_control_frames()

    print()
    if failures:
        print(f"{len(failures)} test(s) failed: {', '.join(failures)}")
        raise SystemExit(1)
    print("All tests passed.")
