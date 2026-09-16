"""Minimal protobuf encoder shared by the tests.

Only needed to build MEXC frames by hand; the scraper itself never encodes
protobuf, it only reads it.
"""

from __future__ import annotations


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


def build_mexc_deals_frame(
    symbol: str = "ETHUSDC",
    price: str = "2500.10",
    qty: str = "1.5",
    trade_type: int = 1,
    ts: int = 1700000000123,
    trade_id: str | None = "T-42",
    aggregated: bool = True,
) -> bytes:
    """A PushDataV3ApiWrapper carrying trades.

    aggregated=True  -> publicAggreDeals, wrapper field 314, item has tradeId
    aggregated=False -> publicDeals,      wrapper field 301, item has no id

    MEXC blocks the plain channel in practice, but both shapes are exercised so
    the parser keeps handling either.

    PublicAggreDealsV3ApiItem { 1: price, 2: quantity, 3: tradeType, 4: time,
                                5: tradeId }
    """
    item = (
        field_str(1, price)
        + field_str(2, qty)
        + field_varint(3, trade_type)
        + field_varint(4, ts)
    )
    if aggregated and trade_id is not None:
        item += field_str(5, trade_id)
    deals = field_msg(1, item)

    if aggregated:
        wrapper = field_str(1, f"spot@public.aggre.deals.v3.api.pb@100ms@{symbol}")
        wrapper += field_msg(314, deals)
    else:
        wrapper = field_str(1, f"spot@public.deals.v3.api.pb@{symbol}")
        wrapper += field_msg(301, deals)
    wrapper += field_str(3, symbol)
    wrapper += field_varint(6, ts)
    return wrapper
