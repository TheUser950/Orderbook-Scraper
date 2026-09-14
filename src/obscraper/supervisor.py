"""Per-exchange lifecycle: connect, supervise, reconnect on failure.

Every exchange runs in its own supervisor task. No error in one adapter may
touch another or terminate the process - that is the core of the error
isolation needed for unattended long-running operation.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

import aiohttp

from .config import AppConfig, ExchangeConfig
from .exchanges.base import ExchangeAdapter
from .latency import race_endpoints
from .storage.base import Writer

log = logging.getLogger(__name__)


class ExchangeSupervisor:
    """Keeps exactly one adapter running and reports events to the writer."""

    def __init__(
        self,
        adapter: ExchangeAdapter,
        cfg: ExchangeConfig,
        app: AppConfig,
        writer: Writer,
        session: aiohttp.ClientSession,
    ) -> None:
        self.adapter = adapter
        self.cfg = cfg
        self.app = app
        self.writer = writer
        self.session = session
        self._stop = asyncio.Event()
        self._backoff = app.connection.reconnect_backoff_min_s
        self.adapter.conn_interval_ms = app.general.interval_ms

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        await self._validate_symbols()
        if not self.adapter.has_active_symbols():
            log.warning(
                "%s: none of the configured symbols are listed there - exchange "
                "skipped, no data will be produced.",
                self.adapter.name,
            )
            self.writer.submit_event(self.adapter.name, "skipped_no_listing")
            return

        transport = self._resolve_transport()
        if transport == "rest":
            await self._run_rest_supervised()
        elif self.cfg.transport == "auto":
            await self._run_auto()
        else:
            # transport=ws is a deliberate choice: no silent switch to REST,
            # just endless reconnect attempts.
            await self._run_ws_supervised()

    async def _run_auto(self) -> None:
        """WebSocket preferred, REST as the safety net.

        If the WebSocket fails repeatedly - for instance because an exchange
        changed its protocol - polling over REST takes over for a bounded
        window before the WebSocket is tried again. That way the dataset does
        not break off on a protocol change, and as soon as the exchange (or an
        update to this scraper) makes the WebSocket usable again, it switches
        back on its own.
        """
        limit = self.app.connection.ws_failures_before_rest
        fallback_s = self.app.connection.rest_fallback_duration_s

        while not self._stop.is_set():
            exhausted = await self._run_ws_supervised(max_consecutive_failures=limit)
            if self._stop.is_set() or not exhausted:
                return

            log.warning(
                "%s: WebSocket failed %d times in a row - switching to REST "
                "polling for %.0fs.",
                self.adapter.name,
                limit,
                fallback_s,
            )
            self.writer.submit_event(
                self.adapter.name,
                "fallback_to_rest",
                detail=f"after {limit} failed WS attempts: {self.adapter.last_error}",
            )

            await self._run_rest_for(fallback_s)
            if self._stop.is_set():
                return

            log.info("%s: trying the WebSocket again.", self.adapter.name)
            self.writer.submit_event(self.adapter.name, "retry_ws")
            self._backoff = self.app.connection.reconnect_backoff_min_s

    async def _run_rest_for(self, duration_s: float) -> None:
        """REST polling for a bounded time (the fallback window)."""
        task = asyncio.create_task(self._run_rest_supervised())
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=duration_s)
        except TimeoutError:
            pass
        except asyncio.CancelledError:
            task.cancel()
            raise
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _validate_symbols(self) -> None:
        await self.adapter.validate_symbols(self.session)
        for sym in self.adapter.symbols:
            if sym.listed is False:
                log.warning(
                    "%s: %s (%s) is not listed - skipping it.",
                    self.adapter.name,
                    sym.canonical,
                    sym.native,
                )
            elif sym.listed is None:
                log.warning(
                    "%s: could not verify the listing of %s (%s) - trying anyway.",
                    self.adapter.name,
                    sym.canonical,
                    sym.note,
                )

    def _resolve_transport(self) -> str:
        wanted = self.cfg.transport
        if wanted == "ws" and not self.adapter.SUPPORTS_WS:
            log.warning(
                "%s: transport=ws configured, but this adapter only supports "
                "REST. Using REST.",
                self.adapter.name,
            )
            return "rest"
        if wanted == "rest":
            return "rest"
        if wanted == "auto":
            return "ws" if self.adapter.SUPPORTS_WS else "rest"
        return "ws"

    # -- WebSocket path ----------------------------------------------------

    async def _run_ws_supervised(
        self, max_consecutive_failures: int | None = None
    ) -> bool:
        """Keep the WebSocket connection alive.

        Returns True when it gave up because of ``max_consecutive_failures``
        (the caller can then fall back to REST), otherwise False.
        """
        endpoint = await self._pick_endpoint()
        relatency_task: asyncio.Task | None = None
        if (
            self.app.connection.relatency_interval_s > 0
            and len(self.cfg.ws_endpoints or self.adapter.WS_ENDPOINTS) > 1
        ):
            relatency_task = asyncio.create_task(
                self._relatency_loop(), name=f"{self.adapter.name}-relatency"
            )

        consecutive_failures = 0
        try:
            while not self._stop.is_set():
                current = getattr(self, "_current_endpoint", endpoint)
                started = time.monotonic()
                try:
                    self.writer.submit_event(self.adapter.name, "connecting", current)
                    watchdog = asyncio.create_task(self._staleness_watchdog())
                    try:
                        await self.adapter.run_ws(
                            current, self.session, on_connect=self._on_connect
                        )
                    finally:
                        watchdog.cancel()
                    consecutive_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    consecutive_failures += 1
                    self.adapter.last_error = f"{type(exc).__name__}: {exc}"
                    log.warning(
                        "%s: WS connection lost (%s): %s",
                        self.adapter.name,
                        current,
                        exc,
                    )
                    self.writer.submit_event(
                        self.adapter.name,
                        "disconnected",
                        current,
                        f"{type(exc).__name__}: {exc}",
                    )

                if self._stop.is_set():
                    break

                if (
                    max_consecutive_failures is not None
                    and consecutive_failures >= max_consecutive_failures
                ):
                    return True

                uptime = time.monotonic() - started
                if uptime > self.app.connection.reconnect_backoff_max_s * 2:
                    # Ran stable for a long time -> reset the backoff.
                    self._backoff = self.app.connection.reconnect_backoff_min_s

                self.adapter.reconnects += 1
                delay = self._backoff * (1 + random.random() * 0.3)
                self._backoff = min(
                    self._backoff * 2, self.app.connection.reconnect_backoff_max_s
                )
                log.info(
                    "%s: reconnecting in %.1fs (attempt #%d)",
                    self.adapter.name,
                    delay,
                    self.adapter.reconnects,
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
            return False
        finally:
            if relatency_task is not None:
                relatency_task.cancel()

    def _on_connect(self, endpoint: str) -> None:
        self._current_endpoint = endpoint
        self.writer.submit_event(self.adapter.name, "connected", endpoint)

    async def _pick_endpoint(self) -> str:
        candidates = self.cfg.ws_endpoints or self.adapter.WS_ENDPOINTS
        if not candidates:
            raise RuntimeError(f"{self.adapter.name}: no WS endpoints defined.")
        if not self.app.connection.pick_fastest_endpoint or len(candidates) == 1:
            self._current_endpoint = candidates[0]
            return candidates[0]

        winner, results = await race_endpoints(
            self.adapter, self.app.connection, self.session
        )
        for r in results:
            self.writer.submit_latency(
                self.adapter.name,
                r.endpoint,
                r.handshake_ms,
                r.first_msg_ms,
                chosen=(r.endpoint == winner),
            )
        self._current_endpoint = winner
        return winner

    async def _relatency_loop(self) -> None:
        interval = self.app.connection.relatency_interval_s
        improve_pct = self.app.connection.relatency_improvement_pct
        while True:
            await asyncio.sleep(interval)
            try:
                winner, results = await race_endpoints(
                    self.adapter, self.app.connection, self.session
                )
            except Exception as exc:
                log.debug("%s: re-evaluation failed: %s", self.adapter.name, exc)
                continue
            current = getattr(self, "_current_endpoint", None)
            if not results or winner == current:
                continue
            by_endpoint = {r.endpoint: r for r in results}
            cur_score = by_endpoint[current].score() if current in by_endpoint else None
            new_score = by_endpoint[winner].score()
            if cur_score is None or cur_score == float("inf"):
                continue
            if new_score < cur_score * (1 - improve_pct / 100):
                log.info(
                    "%s: switching endpoint %s -> %s (clear improvement)",
                    self.adapter.name,
                    current,
                    winner,
                )
                self._current_endpoint = winner
                # The running run_ws task will reconnect on the next connection
                # hiccup anyway; a seamless switch without a data gap would
                # require a second parallel connection - unnecessary here.

    async def _staleness_watchdog(self) -> None:
        threshold = self.app.connection.stale_after_s
        while True:
            await asyncio.sleep(threshold / 2)
            staleness = self.adapter.staleness()
            if staleness is not None and staleness > threshold:
                log.warning(
                    "%s: no updates for %.1fs - forcing a reconnect.",
                    self.adapter.name,
                    staleness,
                )
                self.writer.submit_event(self.adapter.name, "stale_forced_reconnect")
                raise RuntimeError("stale connection")

    # -- REST path ---------------------------------------------------------

    async def _run_rest_supervised(self) -> None:
        while not self._stop.is_set():
            try:
                self.writer.submit_event(self.adapter.name, "rest_polling_start")
                await self.adapter.run_rest(self.session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.adapter.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("%s: REST polling error: %s", self.adapter.name, exc)
                self.writer.submit_event(
                    self.adapter.name, "rest_error", detail=str(exc)
                )
            if self._stop.is_set():
                break
            self.adapter.reconnects += 1
            delay = self._backoff
            self._backoff = min(
                self._backoff * 2, self.app.connection.reconnect_backoff_max_s
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                pass
