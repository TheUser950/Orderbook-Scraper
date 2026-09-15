"""Data structures passed between adapters, sampler and writer."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# A level is always (price, quantity) as strings - exactly as the exchange
# delivered them. Deliberately not float: the conversion would be lossy and is
# unnecessary on the write path anyway. Parsing happens at analysis time.
Level = tuple[str, str]

FLAG_STALE = 1 << 0  # book older than connection.stale_after_s
FLAG_CROSSED = 1 << 1  # best bid >= best ask
FLAG_PARTIAL = 1 << 2  # fewer levels than requested


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class BookState:
    """Immutable state of a top-N book at one point in time.

    Adapters build a new instance on every update and assign it instead of
    mutating the existing one. That way the sampler always sees a internally
    consistent state without any locking.
    """

    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    ts_recv: int
    ts_exchange: int | None = None
    seq: int | None = None
    transport: str = "ws"
    endpoint: str | None = None


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """One recorded row - for the snapshots table or for book_updates.

    Both tables carry the same columns; only ``ts_grid`` is specific to grid
    sampling and ``is_snapshot`` to the stream table.
    """

    ts_grid: int
    ts_local: int
    ts_exchange: int | None
    ts_recv: int
    age_ms: int
    exchange: str
    symbol: str
    exchange_symbol: str
    depth: int
    bids_json: str
    asks_json: str
    seq: int | None
    transport: str
    endpoint: str | None
    flags: int
    # Stream mode only: was this update a full snapshot or a delta? Tells you
    # when a locally maintained book was reset.
    is_snapshot: bool = True


def _to_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


class IncrementalBook:
    """Order book maintained locally from snapshot + deltas.

    Used by the exchanges that do not push a ready-made top-N (OKX, Bitget,
    Bybit, Coinbase). Levels are kept in dicts keyed by Decimal price, so
    applying a delta is O(1) per level and does not re-sort anything.

    Sorting happens in :meth:`top`, which the adapter calls once per incoming
    update. For a deep book like Coinbase's that is a heap selection over
    several thousand entries per update - noticeable but well within budget at
    the observed rates. Deferring it to sampling time would only pay off when
    the sampling grid is coarser than the exchange's update rate.
    """

    __slots__ = ("bids", "asks")

    def __init__(self) -> None:
        # Decimal price -> (price string, quantity string)
        self.bids: dict[Decimal, Level] = {}
        self.asks: dict[Decimal, Level] = {}

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def apply(self, bids: list[Level], asks: list[Level]) -> None:
        self._apply_side(self.bids, bids)
        self._apply_side(self.asks, asks)

    @staticmethod
    def _apply_side(side: dict[Decimal, Level], levels: list[Level]) -> None:
        for price, qty in levels:
            key = _to_decimal(price)
            if _to_decimal(qty) == 0:
                side.pop(key, None)
            else:
                side[key] = (price, qty)

    def top(self, n: int) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
        bids = tuple(v for _, v in heapq.nlargest(n, self.bids.items()))
        asks = tuple(v for _, v in heapq.nsmallest(n, self.asks.items()))
        return bids, asks

    def __len__(self) -> int:
        return len(self.bids) + len(self.asks)


def is_crossed(bids: tuple[Level, ...], asks: tuple[Level, ...]) -> bool:
    if not bids or not asks:
        return False
    return _to_decimal(bids[0][0]) >= _to_decimal(asks[0][0])
