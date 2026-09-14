"""Periodischer Statusbericht ins Log - der schnelle Blick, ob alles noch laeuft."""

from __future__ import annotations

import asyncio
import logging

from .exchanges.base import ExchangeAdapter
from .sampler import Sampler

log = logging.getLogger(__name__)


async def health_loop(
    adapters: list[ExchangeAdapter], sampler: Sampler, interval_s: float, stop: asyncio.Event
) -> None:
    if interval_s <= 0:
        return
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
            break
        except TimeoutError:
            pass
        report(adapters, sampler)


def report(adapters: list[ExchangeAdapter], sampler: Sampler) -> None:
    lines = [f"--- Status nach {sampler.ticks} Ticks ---"]
    for a in adapters:
        if not a.has_active_symbols():
            lines.append(f"  {a.name:10s} uebersprungen (kein Listing)")
            continue
        staleness = a.staleness()
        stale_str = f"{staleness:.1f}s" if staleness is not None else "nie"
        lines.append(
            f"  {a.name:10s} {'UP  ' if a.connected else 'DOWN'} "
            f"transport={a.transport:4s} depth={a.effective_depth:<3d} "
            f"msgs={a.messages:<8d} reconnects={a.reconnects:<3d} "
            f"letztes_update={stale_str:<8s} "
            f"{('err=' + a.last_error) if a.last_error else ''}"
        )
    log.info("\n".join(lines))
