"""Minimaler Protobuf-Wire-Format-Leser (keine Abhaengigkeiten).

MEXC liefert seine Marktdaten seit der V3-Umstellung nur noch als Protobuf.
Statt alle 16 .proto-Dateien zu vendoren, protoc als Build-Schritt
einzufuehren und generierte _pb2.py-Dateien einzuchecken, wird hier das
Wire-Format direkt gelesen - fuer die drei benoetigten Message-Typen
(Wrapper, Depth-Liste, Level-Item) sind das ein paar Dutzend Zeilen.

Der wichtigere Grund: Dieser Leser ist robust gegenueber additiven
Schema-Aenderungen. Unbekannte Feldnummern werden anhand ihres Wire-Typs
uebersprungen, ohne dass das Parsen fehlschlaegt - genau das, was passiert,
wenn eine Boerse ihrem Protokoll neue Felder hinzufuegt.

Bewusste Grenze: Werden bestehende Feldnummern *umnummeriert* (echter
Breaking Change), bricht das hier genauso wie generierter Code. Deshalb
faellt der MEXC-Adapter in dem Fall automatisch auf REST zurueck.

Referenz-Schema: https://github.com/mexcdevelop/websocket-proto
    PushDataV3ApiWrapper   { 1: channel, 3: symbol, 5: createTime,
                             6: sendTime, 303: publicLimitDepths,
                             313: publicAggreDepths }
    PublicLimitDepthsV3Api { 1: asks[], 2: bids[], 3: eventType, 4: version }
    PublicLimitDepthV3ApiItem { 1: price, 2: quantity }
"""

from __future__ import annotations

from typing import Iterator

WIRE_VARINT = 0
WIRE_64BIT = 1
WIRE_LENGTH = 2
WIRE_32BIT = 5


class ProtobufError(ValueError):
    """Frame ist kein lesbares Protobuf (abgeschnitten oder fremdes Format)."""


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    n = len(data)
    while True:
        if pos >= n:
            raise ProtobufError("Varint reicht ueber das Frame-Ende hinaus")
        if shift > 63:
            raise ProtobufError("Varint laenger als 10 Bytes")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def iter_fields(data: bytes) -> Iterator[tuple[int, int, int | bytes]]:
    """Liefert (feldnummer, wire_type, rohwert) fuer jedes Feld im Frame."""
    pos = 0
    n = len(data)
    while pos < n:
        key, pos = _read_varint(data, pos)
        field_no = key >> 3
        wire = key & 0x07
        if field_no == 0:
            raise ProtobufError("Feldnummer 0 ist ungueltig")

        if wire == WIRE_VARINT:
            value, pos = _read_varint(data, pos)
        elif wire == WIRE_64BIT:
            if pos + 8 > n:
                raise ProtobufError("64-Bit-Feld reicht ueber das Frame-Ende hinaus")
            value = int.from_bytes(data[pos : pos + 8], "little")
            pos += 8
        elif wire == WIRE_LENGTH:
            length, pos = _read_varint(data, pos)
            if pos + length > n:
                raise ProtobufError("Laengenfeld reicht ueber das Frame-Ende hinaus")
            value = data[pos : pos + length]
            pos += length
        elif wire == WIRE_32BIT:
            if pos + 4 > n:
                raise ProtobufError("32-Bit-Feld reicht ueber das Frame-Ende hinaus")
            value = int.from_bytes(data[pos : pos + 4], "little")
            pos += 4
        else:
            # Wire-Typ 3/4 sind die abgeschafften Groups; alles andere ist Muell.
            raise ProtobufError(f"Unbekannter Wire-Typ {wire} in Feld {field_no}")

        yield field_no, wire, value


def parse_message(data: bytes) -> dict[int, list[int | bytes]]:
    """Frame -> {feldnummer: [werte]}. Wiederholte Felder behalten ihre Reihenfolge."""
    out: dict[int, list[int | bytes]] = {}
    for field_no, _wire, value in iter_fields(data):
        out.setdefault(field_no, []).append(value)
    return out


def get_str(msg: dict[int, list[int | bytes]], field_no: int) -> str | None:
    values = msg.get(field_no)
    if not values or not isinstance(values[-1], (bytes, bytearray)):
        return None
    return bytes(values[-1]).decode("utf-8", "replace")


def get_int(msg: dict[int, list[int | bytes]], field_no: int) -> int | None:
    values = msg.get(field_no)
    if not values or not isinstance(values[-1], int):
        return None
    return values[-1]


def get_bytes(msg: dict[int, list[int | bytes]], field_no: int) -> bytes | None:
    values = msg.get(field_no)
    if not values or not isinstance(values[-1], (bytes, bytearray)):
        return None
    return bytes(values[-1])


def get_submessages(
    msg: dict[int, list[int | bytes]], field_no: int
) -> list[dict[int, list[int | bytes]]]:
    """Alle Wiederholungen eines eingebetteten Message-Feldes, in Reihenfolge."""
    out = []
    for value in msg.get(field_no, []):
        if isinstance(value, (bytes, bytearray)):
            out.append(parse_message(bytes(value)))
    return out
