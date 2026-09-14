"""Name -> Adapter-Klasse. run.py und der Dry-Run schauen nur hier hin."""

from __future__ import annotations

from .base import ExchangeAdapter
from .binance import BinanceAdapter
from .bingx import BingXAdapter
from .bitget import BitgetAdapter
from .bybit import BybitAdapter
from .coinbase import CoinbaseAdapter
from .gate import GateAdapter
from .htx import HtxAdapter
from .kucoin import KucoinAdapter
from .mexc import MexcAdapter
from .okx import OkxAdapter

REGISTRY: dict[str, type[ExchangeAdapter]] = {
    "binance": BinanceAdapter,
    "okx": OkxAdapter,
    "bybit": BybitAdapter,
    "bitget": BitgetAdapter,
    "kucoin": KucoinAdapter,
    "gate": GateAdapter,
    "htx": HtxAdapter,
    "coinbase": CoinbaseAdapter,
    "bingx": BingXAdapter,
    "mexc": MexcAdapter,
}


def create_adapter(name: str, cfg, conn) -> ExchangeAdapter:
    cls = REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"Unbekannte Boerse '{name}'. Bekannt: {', '.join(sorted(REGISTRY))}"
        )
    return cls(cfg, conn)
