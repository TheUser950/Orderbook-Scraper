#!/usr/bin/env python3
"""Entrypoint des Orderbook-Scrapers.

    python run.py                  # laeuft, bis Ctrl+C / SIGTERM
    python run.py --duration 120   # stoppt nach 120s (Tests)
    python run.py --dry-run        # prueft Config/Listings/Latenz, schreibt nichts
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
        log_dir / "scraper.log", maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    # aiohttp/websockets sind auf DEBUG sehr geschwaetzig.
    logging.getLogger("websockets").setLevel(logging.INFO)
    logging.getLogger("aiohttp").setLevel(logging.INFO)


async def dry_run(app: AppConfig) -> None:
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        rows = []
        for ex_cfg in app.enabled_exchanges():
            adapter = create_adapter(ex_cfg.name, ex_cfg, app.connection)
            await adapter.validate_symbols(session)
            for sym in adapter.symbols:
                listed = (
                    "OK" if sym.listed else ("nicht gelistet" if sym.listed is False else f"unklar ({sym.note})")
                )
                endpoint_info = "-"
                if adapter.SUPPORTS_WS and sym.listed is not False:
                    candidates = ex_cfg.ws_endpoints or adapter.WS_ENDPOINTS
                    if app.connection.pick_fastest_endpoint and len(candidates) > 1:
                        try:
                            winner, results = await race_endpoints(adapter, app.connection, session)
                            endpoint_info = winner
                            for r in results:
                                mark = "*" if r.endpoint == winner else " "
                                print(
                                    f"    {mark} {r.endpoint:55s} "
                                    f"handshake={r.handshake_ms or -1:6.0f}ms "
                                    f"first_msg={(r.first_msg_ms if r.first_msg_ms is not None else -1):6.0f}ms "
                                    f"{'' if r.ok else '(' + r.error[:60] + ')'}"
                                )
                        except Exception as exc:
                            endpoint_info = f"Fehler: {exc}"
                    elif candidates:
                        endpoint_info = candidates[0]
                transport = ex_cfg.transport
                if transport == "auto":
                    transport = "ws" if adapter.SUPPORTS_WS else "rest"
                rows.append(
                    (ex_cfg.name, sym.canonical, sym.native, listed, transport, adapter.effective_depth, endpoint_info)
                )

        print("\n" + "=" * 100)
        print(f"{'Boerse':10s} {'Symbol':10s} {'Nativ':14s} {'Listing':22s} {'Transport':9s} {'Tiefe':5s} Endpunkt")
        print("-" * 100)
        for row in rows:
            ex, canon, native, listed, transport, depth, endpoint = row
            print(f"{ex:10s} {canon:10s} {native:14s} {listed:22s} {transport:9s} {depth:<5d} {endpoint}")
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
                log.info("Beende - fahre alle Verbindungen sauber herunter ...")
                stop.set()
                for sup in supervisors:
                    sup.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                signal.signal(sig, lambda *_: _request_stop())  # Windows-Fallback

        sampler = Sampler(adapters, writer, app.general.interval_ms)

        tasks = [asyncio.create_task(sup.run(), name=f"sup-{sup.adapter.name}") for sup in supervisors]
        tasks.append(asyncio.create_task(sampler.run(stop), name="sampler"))
        tasks.append(
            asyncio.create_task(
                health_loop(adapters, sampler, app.connection.health_report_interval_s, stop),
                name="health",
            )
        )

        if duration is not None:
            async def _timer() -> None:
                await asyncio.sleep(duration)
                _request_stop()

            tasks.append(asyncio.create_task(_timer(), name="duration-timer"))

        log.info(
            "Scraper laeuft: %d Boersen, interval=%dms, depth=%d, symbole=%s",
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
    parser = argparse.ArgumentParser(description="Orderbook-Scraper fuer Krypto-Boersen")
    parser.add_argument("--config", default="config.yaml", help="Pfad zur config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Nur pruefen, nichts schreiben")
    parser.add_argument("--duration", type=float, default=None, help="Nach N Sekunden automatisch stoppen")
    args = parser.parse_args()

    try:
        app = load_config(args.config)
    except ConfigError as exc:
        print(f"Konfigurationsfehler: {exc}", file=sys.stderr)
        return 1

    setup_logging(app.general.log_level)
    log.info("Orderbook-Scraper v%s, Config: %s", __version__, app.source_path)

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
