"""Wall-clock paced sampler.

Scheduling runs on ``time.monotonic()``, not ``time.time()``: the wall clock
can jump forwards or backwards through NTP corrections or - especially in
virtualised environments - hypervisor time adjustments. A scheduler built
directly on it can fire ticks twice or skip them. The monotonic clock is
immune to that; it is anchored against the wall clock once so ``ts_grid``
remains a real Unix timestamp that is comparable across exchanges.
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

        # The first tick lands on the next absolute wall-clock boundary, so
        # every ts_grid is a whole multiple of interval_ms. That matters across
        # runs: anchoring to an arbitrary start instant would give each run its
        # own offset and the timestamps would no longer line up between them.
        # Timestamps are stepped as integer milliseconds to stay exact.
        first_ts_grid = (int(origin_wall * 1000) // self.interval_ms + 1) * self.interval_ms
        first_mono = origin_mono + (first_ts_grid / 1000 - origin_wall)

        k = 0
        while not stop.is_set():
            target_mono = first_mono + k * grid_s
            delay = target_mono - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    break
                except TimeoutError:
                    pass
            if stop.is_set():
                break

            self._sample_once(first_ts_grid + k * self.interval_ms)
            self.ticks += 1

            k += 1
            # If the loop missed several ticks (e.g. because something blocked
            # for a while), skip ahead to the next upcoming tick instead of
            # firing the missed ones back to back (no burst).
            min_k = math.floor((time.monotonic() - first_mono) / grid_s) + 1
            if k < min_k:
                k = min_k

    def _sample_once(self, ts_grid: int) -> None:
        for adapter in self.adapters:
            for sym in adapter.active_symbols():
                snap = adapter.snapshot(sym, ts_grid)
                if snap is not None:
                    self.writer.submit_snapshot(snap)
                    self.snapshots_written += 1
