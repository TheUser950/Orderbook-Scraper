"""Latency racing: pick the fastest of several WS endpoints per exchange.

What is measured is not just the handshake but - more meaningfully - the time
until the first real market data message after subscribing. An endpoint can
answer quickly and still have a sluggish data pipeline behind it.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from dataclasses import dataclass

import aiohttp
from websockets.asyncio.client import connect as ws_connect

from .config import ConnectionConfig
from .exchanges.base import ExchangeAdapter

log = logging.getLogger(__name__)


@dataclass(slots=True)
class EndpointResult:
    endpoint: str
    handshake_ms: float | None
    first_msg_ms: float | None
    ok: bool
    error: str = ""

    def score(self) -> float:
        """Lower is better. Failed endpoints sort to the end."""
        if not self.ok:
            return float("inf")
        return (
            self.first_msg_ms
            if self.first_msg_ms is not None
            else (self.handshake_ms or float("inf"))
        )


async def _probe_once(
    adapter: ExchangeAdapter,
    endpoint: str,
    conn: ConnectionConfig,
    session: aiohttp.ClientSession,
) -> EndpointResult:
    t0 = time.perf_counter()
    try:
        # Go through ws_url() so exchanges that build their URL dynamically
        # (Binance combined streams, KuCoin's token bootstrap) are probed
        # correctly.
        url = await adapter.ws_url(endpoint, session)
        async with ws_connect(
            url,
            open_timeout=conn.latency_probe_timeout_s,
            close_timeout=2,
        ) as ws:
            handshake_ms = (time.perf_counter() - t0) * 1000

            for payload in adapter.subscribe_payloads():
                await adapter._send(ws, payload)  # noqa: SLF001 - internal helper, reused on purpose

            t1 = time.perf_counter()
            first_msg_ms: float | None = None
            deadline = t1 + conn.latency_probe_timeout_s
            while time.perf_counter() < deadline:
                remaining = deadline - time.perf_counter()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except TimeoutError:
                    break
                msg = adapter.decode_frame(raw)
                if adapter.reactive_reply(msg) is not None:
                    continue  # a server ping does not count as market data
                if adapter.parse(msg):
                    first_msg_ms = (time.perf_counter() - t1) * 1000
                    break

            return EndpointResult(endpoint, handshake_ms, first_msg_ms, ok=True)
    except Exception as exc:
        return EndpointResult(endpoint, None, None, ok=False, error=str(exc))


async def race_endpoints(
    adapter: ExchangeAdapter, conn: ConnectionConfig, session: aiohttp.ClientSession
) -> tuple[str, list[EndpointResult]]:
    """Measure all WS_ENDPOINTS in parallel and return the fastest."""
    candidates = adapter.cfg.ws_endpoints or adapter.WS_ENDPOINTS
    if len(candidates) <= 1:
        endpoint = candidates[0] if candidates else ""
        return endpoint, []

    rounds = max(1, conn.latency_probe_rounds)
    all_results: dict[str, list[EndpointResult]] = {c: [] for c in candidates}

    for _ in range(rounds):
        results = await asyncio.gather(
            *(_probe_once(adapter, c, conn, session) for c in candidates)
        )
        for r in results:
            all_results[r.endpoint].append(r)

    summary: list[EndpointResult] = []
    for endpoint, results in all_results.items():
        ok_results = [r for r in results if r.ok]
        if not ok_results:
            summary.append(results[-1])
            continue
        med_handshake = (
            statistics.median(
                r.handshake_ms for r in ok_results if r.handshake_ms is not None
            )
            if any(r.handshake_ms is not None for r in ok_results)
            else None
        )
        first_msgs = [r.first_msg_ms for r in ok_results if r.first_msg_ms is not None]
        med_first = statistics.median(first_msgs) if first_msgs else None
        summary.append(EndpointResult(endpoint, med_handshake, med_first, ok=True))

    summary.sort(key=lambda r: r.score())
    winner = summary[0]

    if not winner.ok:
        log.warning(
            "%s: no endpoint answered the latency probe, using the first candidate.",
            adapter.name,
        )
        return candidates[0], summary

    log.info(
        "%s: fastest endpoint %s (handshake=%.0fms, first_msg=%s)",
        adapter.name,
        winner.endpoint,
        winner.handshake_ms or -1,
        f"{winner.first_msg_ms:.0f}ms" if winner.first_msg_ms is not None else "n/a",
    )
    return winner.endpoint, summary
