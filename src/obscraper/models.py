"""Datenstrukturen, die zwischen Adaptern, Sampler und Writer wandern."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# Ein Level ist immer (Preis, Menge) als String - exakt so, wie die Boerse es
# geliefert hat. Bewusst kein float: die Konvertierung waere verlustbehaftet und
# auf dem Schreibpfad ohnehin unnoetig. Geparst wird erst in der Auswertung.
Level = tuple[str, str]

FLAG_STALE = 1 << 0  # Buch aelter als connection.stale_after_s
FLAG_CROSSED = 1 << 1  # bester Bid >= bester Ask
FLAG_PARTIAL = 1 << 2  # weniger Level als angefordert


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class BookState:
    """Unveraenderlicher Zustand eines Top-N-Buchs zu einem Zeitpunkt.

    Adapter erzeugen bei jedem Update eine neue Instanz und weisen sie zu,
    statt die bestehende zu mutieren. Dadurch sieht der Sampler immer einen
    in sich konsistenten Zustand, ohne dass gesperrt werden muss.
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
    """Eine Zeile in der snapshots-Tabelle."""

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


def _to_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


class IncrementalBook:
    """Lokal gepflegtes Orderbook aus Snapshot + Deltas.

    Genutzt von den Boersen, die kein fertiges Top-N pushen (OKX, Bitget,
    Bybit, Coinbase). Die Level liegen als dict, sortiert wird bewusst erst
    beim Abruf in :meth:`top` - also einmal pro Sampling-Tick statt bei jedem
    eingehenden Delta. Bei Coinbase mit mehreren tausend Leveln und hoher
    Update-Frequenz ist das der Unterschied zwischen "laeuft nebenbei" und
    "frisst eine CPU".
    """

    __slots__ = ("bids", "asks")

    def __init__(self) -> None:
        # Decimal-Preis -> (Preis-String, Mengen-String)
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
