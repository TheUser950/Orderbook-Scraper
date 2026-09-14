#!/usr/bin/env python3
"""Tests fuer den Protobuf-Wire-Format-Leser und das MEXC-Parsing.

Laeuft ohne Test-Framework:  python tests\\test_protobuf.py

Handgerollte Wire-Format-Dekodierung ist leicht subtil falsch zu bekommen,
deshalb wird hier gegen selbst kodierte Frames geprueft - inklusive der
Faelle, die im Betrieb wirklich zaehlen: unbekannte Felder (additive
Schema-Aenderung) und abgeschnittene Frames.
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


# -- Minimaler Protobuf-Encoder, nur fuer die Tests -----------------------


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
    """Baut einen Frame wie ihn MEXC fuer limit.depth sendet."""
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
    print("\nWire-Format:")
    msg = parse_message(field_str(1, "hallo") + field_varint(2, 300))
    check("String-Feld", get_str(msg, 1) == "hallo", repr(get_str(msg, 1)))
    check("Varint-Feld (mehrbytig)", get_int(msg, 2) == 300, repr(get_int(msg, 2)))
    check("Fehlendes Feld -> None", get_str(msg, 99) is None)

    # Wiederholte Felder muessen ihre Reihenfolge behalten - sonst waere die
    # Level-Sortierung des Orderbooks kaputt.
    repeated = b"".join(field_msg(1, level_item(p, "1")) for p in ["3", "2", "1"])
    subs = get_submessages(parse_message(repeated), 1)
    order = [get_str(s, 1) for s in subs]
    check("Reihenfolge wiederholter Felder", order == ["3", "2", "1"], repr(order))

    for name, data in [
        ("abgeschnittenes Varint", b"\x08\x80"),
        ("Laenge ueber Frame-Ende", b"\x0a\xff\x01ab"),
        ("Feldnummer 0", b"\x00\x01"),
    ]:
        try:
            parse_message(data)
            check(f"{name} wirft ProtobufError", False, "keine Exception")
        except ProtobufError:
            check(f"{name} wirft ProtobufError", True)


def make_adapter(depth: int = 20) -> MexcAdapter:
    cfg = ExchangeConfig(name="mexc", depth=depth, symbols=["ETH/USDC"])
    return MexcAdapter(cfg, ConnectionConfig())


def test_mexc_parse() -> None:
    print("\nMEXC-Frame:")
    adapter = make_adapter()
    updates = adapter.parse(("pb", build_mexc_frame()))
    check("genau ein Update", len(updates) == 1, f"{len(updates)}")
    if not updates:
        return
    upd = updates[0]
    check("Symbol", upd.symbol == "ETHUSDC", upd.symbol)
    check("Bids", upd.bids == [("2500.10", "1.5"), ("2500.00", "3.0")], str(upd.bids))
    check("Asks", upd.asks == [("2500.20", "2.0"), ("2500.30", "4.0")], str(upd.asks))
    check("Exchange-Zeitstempel", upd.ts_exchange == 1700000000123, str(upd.ts_exchange))
    check("Sequenz aus version", upd.seq == 987654, str(upd.seq))
    check("ist Snapshot", upd.is_snapshot is True)

    # Preise muessen exakt als String durchgereicht werden - eine
    # Float-Konvertierung waere auf dem Schreibpfad verlustbehaftet.
    exact = build_mexc_frame(bids=[("0.000000012345678", "9999999.123456789")])
    upd2 = adapter.parse(("pb", exact))[0]
    check(
        "Preis-String unveraendert",
        upd2.bids[0] == ("0.000000012345678", "9999999.123456789"),
        str(upd2.bids[0]),
    )


def test_schema_change_tolerance() -> None:
    """Der eigentliche Punkt: additive Schema-Aenderungen duerfen nicht stoeren."""
    print("\nToleranz gegenueber Schema-Aenderungen:")
    adapter = make_adapter()

    unknown = (
        field_str(42, "ein neues Feld")
        + field_varint(43, 12345)
        + field_msg(44, field_str(1, "verschachtelt neu"))
    )
    updates = adapter.parse(("pb", build_mexc_frame(extra_unknown=unknown)))
    check("unbekannte Felder werden uebersprungen", len(updates) == 1, str(len(updates)))
    if updates:
        check(
            "Daten trotz unbekannter Felder korrekt",
            updates[0].bids == [("2500.10", "1.5"), ("2500.00", "3.0")],
            str(updates[0].bids),
        )

    # Kaputte Frames duerfen nur gezaehlt, nicht geworfen werden - sonst
    # reisst ein einzelnes Frame die ganze Verbindung ab.
    before = adapter.protobuf_errors
    result = adapter.parse(("pb", b"\x0a\xff\xff\xff\x7f"))
    check("kaputtes Frame wirft nicht", result == [], str(result))
    check("Fehlerzaehler erhoeht", adapter.protobuf_errors == before + 1)

    # Ein Wrapper ohne Depth-Body (anderer Kanal) ist kein Fehler.
    other = field_str(1, "spot@public.deals.v3.api.pb@ETHUSDC") + field_str(3, "ETHUSDC")
    errors_before = adapter.protobuf_errors
    check("fremder Kanal -> leer", adapter.parse(("pb", other)) == [])
    check("fremder Kanal ist kein Fehler", adapter.protobuf_errors == errors_before)


def test_depth_truncation() -> None:
    print("\nTiefen-Begrenzung:")
    adapter = make_adapter(depth=5)
    many = [(f"{2500 - i}", "1.0") for i in range(20)]
    upd = adapter.parse(("pb", build_mexc_frame(bids=many, asks=many)))[0]
    check("Bids auf effective_depth gekappt", len(upd.bids) == 5, str(len(upd.bids)))
    check("Asks auf effective_depth gekappt", len(upd.asks) == 5, str(len(upd.asks)))


def test_control_frames() -> None:
    print("\nKontrollnachrichten:")
    adapter = make_adapter()
    check("JSON-Ack liefert keine Updates", adapter.parse({"id": 0, "code": 0}) == [])
    check(
        "Server-PING wird beantwortet",
        adapter.reactive_reply({"id": 0, "code": 0, "msg": "PING"})
        == {"method": "PONG"},
    )
    check("normale Nachricht braucht keine Antwort", adapter.reactive_reply({}) is None)
    check(
        "Binaerframe wird als Protobuf markiert",
        adapter.decode_frame(b"\x08\x01") == ("pb", b"\x08\x01"),
    )
    check(
        "Textframe wird als JSON gelesen",
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
        print(f"{len(failures)} Test(s) fehlgeschlagen: {', '.join(failures)}")
        raise SystemExit(1)
    print("Alle Tests bestanden.")
