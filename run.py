#!/usr/bin/env python3
"""Entry point of the order book scraper.

    python run.py                  # runs until Ctrl+C / SIGTERM
    python run.py --duration 120   # stops after 120s (for testing)
    python run.py --dry-run        # checks config/listings/latency, writes nothing
    python run.py --config other.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import aiohttp  # noqa: E402

from obscraper import __version__  # noqa: E402
from obscraper.config import AppConfig, ConfigError, load_config  # noqa: E402
from obscraper.exchanges.registry import create_adapter  # noqa: E402
from obscraper.health import health_loop, report  # noqa: E402
from obscraper.latency import race_endpoints  # noqa: E402
from obscraper.sampler import Sampler  # noqa: E402
from obscraper.storage.sqlite_writer import SqliteWriter  # noqa: E402
from obscraper.supervisor import ExchangeSupervisor  # noqa: E402

log = logging.getLogger("obscraper")


def setup_logging(level: str) -> None:
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "scraper.log",
        maxBytes=20 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    # aiohttp/websockets are very chatty on DEBUG.
    logging.getLogger("websockets").setLevel(logging.INFO)
    logging.getLogger("aiohttp").setLevel(logging.INFO)


async def dry_run(app: AppConfig) -> None:
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        rows = []
        for ex_cfg in app.enabled_exchanges():
            adapter = create_adapter(ex_cfg.name, ex_cfg, app.connection)
            await adapter.validate_symbols(session)

            transport = ex_cfg.transport
            if transport == "auto":
                transport = "ws" if adapter.SUPPORTS_WS else "rest"

            # Race once per exchange, not once per symbol - the endpoint is a
            # property of the connection, and all symbols share one.
            endpoint_info = "-"
            candidates = ex_cfg.ws_endpoints or adapter.WS_ENDPOINTS
            if adapter.SUPPORTS_WS and adapter.has_active_symbols() and candidates:
                if app.connection.pick_fastest_endpoint and len(candidates) > 1:
                    try:
                        winner, results = await race_endpoints(
                            adapter, app.connection, session
                        )
                        endpoint_info = winner
                        for r in results:
                            mark = "*" if r.endpoint == winner else " "
                            first = r.first_msg_ms if r.first_msg_ms is not None else -1
                            print(
                                f"    {mark} {r.endpoint:55s} "
                                f"handshake={r.handshake_ms or -1:6.0f}ms "
                                f"first_msg={first:6.0f}ms "
                                f"{'' if r.ok else '(' + r.error[:60] + ')'}"
                            )
                    except Exception as exc:
                        endpoint_info = f"error: {exc}"
                else:
                    endpoint_info = candidates[0]

            for sym in adapter.symbols:
                if sym.listed:
                    listed = "OK"
                elif sym.listed is False:
                    listed = "not listed"
                else:
                    listed = f"unclear ({sym.note})"
                rows.append(
                    (
                        ex_cfg.name,
                        sym.canonical,
                        sym.native,
                        listed,
                        transport,
                        adapter.effective_depth,
                        endpoint_info if sym.listed is not False else "-",
                    )
                )

        print("\n" + "=" * 100)
        print(
            f"{'Exchange':10s} {'Symbol':10s} {'Native':14s} {'Listing':22s} "
            f"{'Transport':9s} {'Depth':5s} Endpoint"
        )
        print("-" * 100)
        for ex, canon, native, listed, transport, depth, endpoint in rows:
            print(
                f"{ex:10s} {canon:10s} {native:14s} {listed:22s} "
                f"{transport:9s} {depth:<5d} {endpoint}"
            )
        print("=" * 100)


async def run_forever(app: AppConfig, duration: float | None) -> None:
    writer = SqliteWriter(app.storage)
    config_json, config_hash = app.fingerprint()
    await writer.start(config_json, config_hash, __version__)

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        adapters = [
            create_adapter(ex_cfg.name, ex_cfg, app.connection)
            for ex_cfg in app.enabled_exchanges()
        ]
        supervisors = [
            ExchangeSupervisor(adapter, ex_cfg, app, writer, session)
            for adapter, ex_cfg in zip(adapters, app.enabled_exchanges())
        ]

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _request_stop(*_args) -> None:
            if not stop.is_set():
                log.info("Shutting down - closing all connections cleanly ...")
                stop.set()
                for sup in supervisors:
                    sup.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                signal.signal(sig, lambda *_: _request_stop())  # Windows fallback

        sampler = Sampler(adapters, writer, app.general.interval_ms)

        tasks = [
            asyncio.create_task(sup.run(), name=f"sup-{sup.adapter.name}")
            for sup in supervisors
        ]
        tasks.append(asyncio.create_task(sampler.run(stop), name="sampler"))
        tasks.append(
            asyncio.create_task(
                health_loop(
                    adapters, sampler, app.connection.health_report_interval_s, stop
                ),
                name="health",
            )
        )

        if duration is not None:

            async def _timer() -> None:
                await asyncio.sleep(duration)
                _request_stop()

            tasks.append(asyncio.create_task(_timer(), name="duration-timer"))

        log.info(
            "Scraper running: %d exchanges, interval=%dms, depth=%d, symbols=%s",
            len(adapters),
            app.general.interval_ms,
            app.general.depth,
            ", ".join(app.general.symbols),
        )

        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            report(adapters, sampler)
            await writer.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Order book scraper for crypto exchanges"
    )
    parser.add_argument("--config", default="config.yaml", help="path to config.yaml")
    parser.add_argument(
        "--dry-run", action="store_true", help="only check, write nothing"
    )
    parser.add_argument(
        "--duration", type=float, default=None, help="stop automatically after N seconds"
    )
    args = parser.parse_args()

    try:
        app = load_config(args.config)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    setup_logging(app.general.log_level)
    log.info("Order book scraper v%s, config: %s", __version__, app.source_path)

    try:
        if args.dry_run:
            asyncio.run(dry_run(app))
        else:
            asyncio.run(run_forever(app, args.duration))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
