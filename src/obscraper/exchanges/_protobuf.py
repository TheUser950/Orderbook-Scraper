"""Minimal protobuf wire-format reader (no dependencies).

Since the V3 migration MEXC delivers its market data only as protobuf. Rather
than vendoring all 16 .proto files, adding protoc as a build step and checking
in generated _pb2.py files, the wire format is read directly here - for the
three message types actually needed (wrapper, depth list, level item) that is
a few dozen lines.

The more important reason: this reader is robust against additive schema
changes. Unknown field numbers are skipped based on their wire type without
the parse failing - exactly what happens when an exchange adds new fields to
its protocol.

Deliberate limit: if existing field numbers are *renumbered* (a real breaking
change), this breaks just like generated code would. That is why the MEXC
adapter falls back to REST automatically in that case.

Reference schema: https://github.com/mexcdevelop/websocket-proto
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
    """Frame is not readable protobuf (truncated or a foreign format)."""


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    n = len(data)
    while True:
        if pos >= n:
            raise ProtobufError("varint runs past the end of the frame")
        if shift > 63:
            raise ProtobufError("varint longer than 10 bytes")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def iter_fields(data: bytes) -> Iterator[tuple[int, int, int | bytes]]:
    """Yield (field number, wire type, raw value) for every field in the frame."""
    pos = 0
    n = len(data)
    while pos < n:
        key, pos = _read_varint(data, pos)
        field_no = key >> 3
        wire = key & 0x07
        if field_no == 0:
            raise ProtobufError("field number 0 is invalid")

        if wire == WIRE_VARINT:
            value, pos = _read_varint(data, pos)
        elif wire == WIRE_64BIT:
            if pos + 8 > n:
                raise ProtobufError("64-bit field runs past the end of the frame")
            value = int.from_bytes(data[pos : pos + 8], "little")
            pos += 8
        elif wire == WIRE_LENGTH:
            length, pos = _read_varint(data, pos)
            if pos + length > n:
                raise ProtobufError("length field runs past the end of the frame")
            value = data[pos : pos + length]
            pos += length
        elif wire == WIRE_32BIT:
            if pos + 4 > n:
                raise ProtobufError("32-bit field runs past the end of the frame")
            value = int.from_bytes(data[pos : pos + 4], "little")
            pos += 4
        else:
            # Wire types 3/4 are the removed groups; anything else is garbage.
            raise ProtobufError(f"unknown wire type {wire} in field {field_no}")

        yield field_no, wire, value


def parse_message(data: bytes) -> dict[int, list[int | bytes]]:
    """Frame -> {field number: [values]}. Repeated fields keep their order."""
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
    """All repetitions of one embedded message field, in order."""
    out = []
    for value in msg.get(field_no, []):
        if isinstance(value, (bytes, bytearray)):
            out.append(parse_message(bytes(value)))
    return out
