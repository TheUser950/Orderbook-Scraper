"""MEXC spot.

MEXC retired the JSON WebSocket in 2025; the current channel
``spot@public.limit.depth.v3.api.pb@SYMBOL@N`` delivers protobuf only. It is
decoded with the dependency-free wire-format reader in ``_protobuf.py``
(reasoning and limits documented there).

Control messages (subscribe acknowledgement, PONG) still arrive as JSON text
frames, market data as binary frames - both are handled separately here.

If protobuf parsing fails repeatedly (for instance because MEXC renumbers
fields), the supervisor switches to REST polling automatically in
transport=auto mode. With grid sampling that is almost equivalent in quality,
just with slightly higher latency.
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from ._protobuf import (
    ProtobufError,
    get_int,
    get_str,
    get_submessages,
    parse_message,
)
from .base import (
    BUY,
    SELL,
    BookUpdate,
    ExchangeAdapter,
    Level,
    SymbolStatus,
    TradeUpdate,
    parse_levels,
)

log = logging.getLogger(__name__)

_EXCHANGE_INFO = "https://api.mexc.com/api/v3/exchangeInfo"
_DEPTH = "https://api.mexc.com/api/v3/depth"
_REST_TRADES = "https://api.mexc.com/api/v3/trades"
_LISTED_STATES = {"ENABLED", "TRADING", "1"}

# Field numbers per mexcdevelop/websocket-proto
_WRAP_CHANNEL = 1
_WRAP_SYMBOL = 3
_WRAP_CREATE_TIME = 5
_WRAP_SEND_TIME = 6
_WRAP_LIMIT_DEPTHS = 303  # PublicLimitDepthsV3Api  (top-N snapshot)
_WRAP_AGGRE_DEPTHS = 313  # PublicAggreDepthsV3Api  (aggregated/incremental)
_WRAP_DEALS = 301  # PublicDealsV3Api        (executed trades)
_WRAP_AGGRE_DEALS = 314  # PublicAggreDealsV3Api

_DEPTHS_ASKS = 1
_DEPTHS_BIDS = 2
_DEPTHS_VERSION = 4

_ITEM_PRICE = 1
_ITEM_QUANTITY = 2

# PublicAggreDealsV3Api { 1: deals[] }
# PublicAggreDealsV3ApiItem { 1: price, 2: quantity, 3: tradeType, 4: time,
#                             5: tradeId }
# The plain PublicDealsV3ApiItem has the same layout but no tradeId.
_DEALS_ITEMS = 1
_DEAL_TRADE_TYPE = 3
_DEAL_TIME = 4
_DEAL_TRADE_ID = 5

# MEXC refuses the individual-trade channel with "Blocked!" and only serves the
# aggregated one, which requires an interval in the topic. 100ms is the finest
# it offers.
_AGGRE_DEALS_INTERVAL = "100ms"


class MexcAdapter(ExchangeAdapter):
    name = "mexc"
    WS_ENDPOINTS = ["wss://wbs-api.mexc.com/ws"]
    REST_BASE = "https://api.mexc.com"
    PARTIAL_DEPTHS = [5, 10, 20]
    KEEPALIVE_INTERVAL = 20.0  # the server disconnects after 30s of silence
    MIN_REST_INTERVAL_MS = 500

    def __init__(self, cfg, conn) -> None:
        super().__init__(cfg, conn)
        self.protobuf_errors = 0

    @classmethod
    def native_symbol(cls, canonical: str) -> str:
        base, quote = canonical.split("/")
        return f"{base}{quote}".upper()

    # -- WebSocket ---------------------------------------------------------

    def subscribe_payloads(self) -> list[Any]:
        return [
            {
                "method": "SUBSCRIPTION",
                "params": [
                    f"spot@public.limit.depth.v3.api.pb@{s.native}@{self.effective_depth}"
                    for s in self.symbols
                ],
            }
        ]

    def trade_subscribe_payloads(self) -> list[Any]:
        # The individual-trade channel (spot@public.deals.v3.api.pb@SYMBOL) is
        # refused by MEXC with 'Reason: Blocked!', verified against the live
        # feed, so the aggregated channel is the only one available. Note the
        # interval sits *before* the symbol in this topic name.
        return [
            {
                "method": "SUBSCRIPTION",
                "params": [
                    f"spot@public.aggre.deals.v3.api.pb@{_AGGRE_DEALS_INTERVAL}@{s.native}"
                    for s in self.symbols
                ],
            }
        ]

    def parse_trades(self, msg: Any) -> list[TradeUpdate]:
        if not (isinstance(msg, tuple) and len(msg) == 2 and msg[0] == "pb"):
            return []
        try:
            return self._parse_trades_protobuf(msg[1])
        except ProtobufError as exc:
            self.protobuf_errors += 1
            self.last_error = f"ProtobufError (deals): {exc}"
            return []

    def _parse_trades_protobuf(self, data: bytes) -> list[TradeUpdate]:
        wrapper = parse_message(data)
        body = None
        for field_no in (_WRAP_AGGRE_DEALS, _WRAP_DEALS):
            subs = get_submessages(wrapper, field_no)
            if subs:
                body = subs[-1]
                break
        if body is None:
            return []  # a depth frame or another channel entirely

        native_symbol = get_str(wrapper, _WRAP_SYMBOL)
        if not native_symbol:
            # Symbol is the last segment of the channel name in both the
            # aggregated and the plain topic layouts.
            channel = get_str(wrapper, _WRAP_CHANNEL) or ""
            parts = channel.split("@")
            native_symbol = parts[-1] if parts else ""
        if not native_symbol:
            return []

        out: list[TradeUpdate] = []
        for item in get_submessages(body, _DEALS_ITEMS):
            price = get_str(item, _ITEM_PRICE)
            qty = get_str(item, _ITEM_QUANTITY)
            if price is None or qty is None:
                continue
            trade_type = get_int(item, _DEAL_TRADE_TYPE)
            out.append(
                TradeUpdate(
                    symbol=native_symbol,
                    price=price,
                    qty=qty,
                    # Present on the aggregated channel, absent on the plain
                    # one - hence the None fallback rather than a hard read.
                    trade_id=get_str(item, _DEAL_TRADE_ID),
                    ts_exchange=get_int(item, _DEAL_TIME),
                    side=_side_from_trade_type(trade_type),
                    raw_side=f"tradeType={trade_type}",
                )
            )
        return out

    def keepalive_payload(self) -> Any | None:
        return {"method": "PING"}

    def decode_frame(self, raw: str | bytes) -> Any:
        # Market data arrives binary (protobuf), control messages as JSON text.
        # The binary case is tagged rather than parsed immediately so a broken
        # frame can be caught cleanly in parse().
        if isinstance(raw, (bytes, bytearray)):
            return ("pb", bytes(raw))
        return super().decode_frame(raw)

    def reactive_reply(self, msg: Any) -> Any | None:
        # MEXC expects a PONG reply if the server pings on its own.
        if isinstance(msg, dict) and str(msg.get("msg", "")).upper() == "PING":
            return {"method": "PONG"}
        return None

    def parse(self, msg: Any) -> list[BookUpdate]:
        if not (isinstance(msg, tuple) and len(msg) == 2 and msg[0] == "pb"):
            return []  # JSON control frame (subscribe ack, PONG)
        try:
            return self._parse_protobuf(msg[1])
        except ProtobufError as exc:
            self.protobuf_errors += 1
            self.last_error = f"ProtobufError: {exc}"
            if self.protobuf_errors in (1, 10, 100):
                log.warning(
                    "%s: protobuf frame unreadable (%dx): %s. Did MEXC change "
                    "the schema? If this persists, the REST fallback kicks in "
                    "in transport=auto mode.",
                    self.name,
                    self.protobuf_errors,
                    exc,
                )
            return []

    def _parse_protobuf(self, data: bytes) -> list[BookUpdate]:
        wrapper = parse_message(data)

        body = None
        is_snapshot = True
        for field_no, snapshot in (
            (_WRAP_LIMIT_DEPTHS, True),
            (_WRAP_AGGRE_DEPTHS, False),
        ):
            subs = get_submessages(wrapper, field_no)
            if subs:
                body = subs[-1]
                is_snapshot = snapshot
                break
        if body is None:
            return []  # a different channel in the same wrapper - not our problem

        native_symbol = get_str(wrapper, _WRAP_SYMBOL)
        if not native_symbol:
            # Fallback: the symbol is also part of the channel name.
            channel = get_str(wrapper, _WRAP_CHANNEL) or ""
            parts = channel.split("@")
            native_symbol = parts[-2] if len(parts) >= 2 else ""
        if not native_symbol:
            return []

        return [
            BookUpdate(
                symbol=native_symbol,
                bids=_levels(body, _DEPTHS_BIDS, self.effective_depth),
                asks=_levels(body, _DEPTHS_ASKS, self.effective_depth),
                ts_exchange=get_int(wrapper, _WRAP_SEND_TIME)
                or get_int(wrapper, _WRAP_CREATE_TIME),
                seq=_version_to_int(get_str(body, _DEPTHS_VERSION)),
                is_snapshot=is_snapshot,
            )
        ]

    # -- REST --------------------------------------------------------------

    async def fetch_listed_symbols(self, session: aiohttp.ClientSession) -> set[str]:
        data = await self.get_json(session, _EXCHANGE_INFO)
        return {
            s["symbol"]
            for s in data.get("symbols", [])
            if str(s.get("status", "")).upper() in _LISTED_STATES
        }

    async def rest_depth(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> BookUpdate | None:
        data = await self.get_json(
            session,
            _DEPTH,
            params={"symbol": sym.native, "limit": str(self.effective_depth)},
        )
        return BookUpdate(
            symbol=sym.native,
            bids=parse_levels(data.get("bids"), self.effective_depth),
            asks=parse_levels(data.get("asks"), self.effective_depth),
            seq=data.get("lastUpdateId"),
        )

    async def rest_trades(
        self, session: aiohttp.ClientSession, sym: SymbolStatus
    ) -> list[TradeUpdate]:
        rows = await self.get_json(
            session, _REST_TRADES, params={"symbol": sym.native, "limit": "100"}
        )
        out: list[TradeUpdate] = []
        for e in rows:
            buyer_is_maker = e.get("isBuyerMaker")
            out.append(
                TradeUpdate(
                    symbol=sym.native,
                    price=str(e.get("price")),
                    qty=str(e.get("qty")),
                    # The REST response carries an `id`, but it is routinely
                    # null on MEXC, so it cannot be relied on for dedupe.
                    trade_id=str(e["id"]) if e.get("id") is not None else None,
                    ts_exchange=e.get("time"),
                    side=SELL if buyer_is_maker else BUY,
                    raw_side=f"isBuyerMaker={buyer_is_maker}",
                )
            )
        return out


def _side_from_trade_type(trade_type: int | None) -> str | None:
    """MEXC encodes the taker side as tradeType: 1 = buy, 2 = sell."""
    if trade_type == 1:
        return BUY
    if trade_type == 2:
        return SELL
    return None


def _levels(body: dict, field_no: int, limit: int) -> list[Level]:
    out: list[Level] = []
    for item in get_submessages(body, field_no):
        price = get_str(item, _ITEM_PRICE)
        qty = get_str(item, _ITEM_QUANTITY)
        if price is None or qty is None:
            continue
        out.append((price, qty))
        if len(out) >= limit:
            break
    return out


def _version_to_int(version: str | None) -> int | None:
    try:
        return int(version) if version is not None else None
    except ValueError:
        return None
