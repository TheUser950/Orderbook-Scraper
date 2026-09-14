"""Wall-Clock-getakteter Sampler.

Geplant wird ueber ``time.monotonic()``, nicht ueber ``time.time()``: Die
Wall-Clock kann durch NTP-Korrekturen oder (gerade in virtualisierten
Umgebungen) durch Hypervisor-Zeitanpassungen leicht vor- oder zurueckspringen.
Ein Scheduler, der direkt auf ihr aufbaut, kann dadurch Ticks doppelt feuern
oder auslassen. Die monotone Uhr ist dagegen immun; sie wird einmalig gegen
die Wall-Clock verankert, damit ``ts_grid`` weiterhin ein echter, ueber
Boersen vergleichbarer Unix-Zeitstempel bleibt.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time

from .exchanges.base import ExchangeAdapter
from .storage.base import Writer

log = logging.getLogger(__name__)


class Sampler:
    def __init__(
        self, adapters: list[ExchangeAdapter], writer: Writer, interval_ms: int
    ) -> None:
        self.adapters = adapters
        self.writer = writer
        self.interval_ms = interval_ms
        self.ticks = 0
        self.snapshots_written = 0

    async def run(self, stop: asyncio.Event) -> None:
        grid_s = self.interval_ms / 1000
        origin_wall = time.time()
        origin_mono = time.monotonic()

        # Naechster Tick-Index relativ zum Referenzpunkt.
        next_n = math.floor(origin_mono / grid_s) + 1

        while not stop.is_set():
            target_mono = next_n * grid_s
            delay = target_mono - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    break
                except TimeoutError:
                    pass
            if stop.is_set():
                break

            ts_grid = round((origin_wall + (target_mono - origin_mono)) * 1000)
            self._sample_once(ts_grid)
            self.ticks += 1

            next_n += 1
            # Falls die Schleife (z.B. durch eine lange Blockade) mehrere
            # Ticks verpasst hat: auf den naechsten anstehenden Tick
            # vorspulen statt die verpassten nachzufeuern (kein Burst).
            min_next = math.floor(time.monotonic() / grid_s) + 1
            if next_n < min_next:
                next_n = min_next

    def _sample_once(self, ts_grid: int) -> None:
        for adapter in self.adapters:
            for sym in adapter.active_symbols():
                snap = adapter.snapshot(sym, ts_grid)
                if snap is not None:
                    self.writer.submit_snapshot(snap)
                    self.snapshots_written += 1
