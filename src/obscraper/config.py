"""Laden und Validieren der config.yaml.

Fehlerhafte Konfiguration soll beim Start mit einer verstaendlichen Meldung
abbrechen - nicht drei Stunden spaeter mit einem KeyError im Adapter.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import yaml

TRANSPORTS = ("ws", "rest", "auto")
DEPTH_POLICIES = ("at_least", "at_most", "nearest")
BACKENDS = ("sqlite",)


class ConfigError(Exception):
    """Wird mit einer fuer Menschen lesbaren Meldung geworfen."""


@dataclass(slots=True)
class GeneralConfig:
    symbols: list[str] = field(default_factory=lambda: ["ETH/USDC"])
    interval_ms: int = 1000
    depth: int = 20
    depth_policy: str = "at_least"
    transport: str = "auto"
    log_level: str = "INFO"


@dataclass(slots=True)
class StorageConfig:
    backend: str = "sqlite"
    path: str = "./data/orderbook.db"
    batch_size: int = 200
    flush_interval_ms: int = 1000
    queue_maxsize: int = 20000


@dataclass(slots=True)
class ConnectionConfig:
    pick_fastest_endpoint: bool = True
    latency_probe_timeout_s: float = 5.0
    latency_probe_rounds: int = 2
    relatency_interval_s: int = 3600
    relatency_improvement_pct: float = 25.0
    reconnect_backoff_min_s: float = 1.0
    reconnect_backoff_max_s: float = 60.0
    # Nur fuer transport=auto: nach so vielen WS-Fehlversuchen in Folge wird
    # voruebergehend auf REST-Polling ausgewichen.
    ws_failures_before_rest: int = 3
    rest_fallback_duration_s: float = 600.0
    stale_after_s: float = 15.0
    record_stale_snapshots: bool = True
    ws_ping_interval_s: float = 20.0
    rest_timeout_s: float = 10.0
    health_report_interval_s: float = 60.0


@dataclass(slots=True)
class ExchangeConfig:
    """Aufgeloeste Einstellungen fuer genau eine Boerse.

    Alle Felder sind bereits mit den general-Werten aufgefuellt, damit die
    Adapter nie zwei Stellen befragen muessen.
    """

    name: str
    enabled: bool = True
    depth: int = 20
    depth_policy: str = "at_least"
    transport: str = "auto"
    symbols: list[str] = field(default_factory=list)
    symbol_override: dict[str, str] = field(default_factory=dict)
    ws_endpoints: list[str] | None = None
    rest_interval_ms: int | None = None


@dataclass(slots=True)
class AppConfig:
    general: GeneralConfig
    storage: StorageConfig
    connection: ConnectionConfig
    exchanges: dict[str, ExchangeConfig]
    source_path: str = ""

    def enabled_exchanges(self) -> list[ExchangeConfig]:
        return [e for e in self.exchanges.values() if e.enabled]

    def fingerprint(self) -> tuple[str, str]:
        """(json, sha256) - wird pro Lauf in der runs-Tabelle abgelegt."""
        payload = json.dumps(
            {
                "general": asdict(self.general),
                "storage": asdict(self.storage),
                "connection": asdict(self.connection),
                "exchanges": {k: asdict(v) for k, v in self.exchanges.items()},
            },
            sort_keys=True,
        )
        return payload, hashlib.sha256(payload.encode()).hexdigest()


def _coerce(section: str, raw: dict[str, Any], cls: type) -> Any:
    known = {f for f in cls.__slots__}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"Unbekannte Schluessel in '{section}': {', '.join(sorted(unknown))}. "
            f"Erlaubt sind: {', '.join(sorted(known))}"
        )
    return cls(**raw)


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"Konfigurationsdatei nicht gefunden: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"config.yaml ist kein gueltiges YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError("config.yaml muss auf oberster Ebene ein Mapping sein.")

    unknown_top = set(raw) - {"general", "storage", "connection", "exchanges"}
    if unknown_top:
        raise ConfigError(
            f"Unbekannte Abschnitte in config.yaml: {', '.join(sorted(unknown_top))}"
        )

    general = _coerce("general", raw.get("general") or {}, GeneralConfig)
    storage = _coerce("storage", raw.get("storage") or {}, StorageConfig)
    connection = _coerce("connection", raw.get("connection") or {}, ConnectionConfig)

    _validate_general(general)
    _validate_storage(storage)
    _validate_connection(connection)

    exchanges_raw = raw.get("exchanges") or {}
    if not isinstance(exchanges_raw, dict) or not exchanges_raw:
        raise ConfigError("Abschnitt 'exchanges' fehlt oder ist leer.")

    exchanges: dict[str, ExchangeConfig] = {}
    for name, over in exchanges_raw.items():
        exchanges[name] = _build_exchange(name, over or {}, general)

    if not any(e.enabled for e in exchanges.values()):
        raise ConfigError("Keine einzige Boerse ist aktiviert (enabled: true).")

    return AppConfig(general, storage, connection, exchanges, str(path))


def _build_exchange(
    name: str, over: dict[str, Any], general: GeneralConfig
) -> ExchangeConfig:
    allowed = {
        "enabled",
        "depth",
        "depth_policy",
        "transport",
        "symbols",
        "symbol_override",
        "ws_endpoints",
        "rest_interval_ms",
    }
    if not isinstance(over, dict):
        raise ConfigError(
            f"exchanges.{name} muss ein Mapping sein (z.B. '{{ enabled: true }}')."
        )
    unknown = set(over) - allowed
    if unknown:
        raise ConfigError(
            f"Unbekannte Schluessel unter exchanges.{name}: "
            f"{', '.join(sorted(unknown))}. Erlaubt: {', '.join(sorted(allowed))}"
        )

    cfg = ExchangeConfig(
        name=name,
        enabled=bool(over.get("enabled", True)),
        depth=int(over.get("depth", general.depth)),
        depth_policy=str(over.get("depth_policy", general.depth_policy)),
        transport=str(over.get("transport", general.transport)),
        symbols=list(over.get("symbols", general.symbols)),
        symbol_override=dict(over.get("symbol_override") or {}),
        ws_endpoints=list(over["ws_endpoints"]) if over.get("ws_endpoints") else None,
        rest_interval_ms=(
            int(over["rest_interval_ms"])
            if over.get("rest_interval_ms") is not None
            else None
        ),
    )

    if cfg.transport not in TRANSPORTS:
        raise ConfigError(
            f"exchanges.{name}.transport = '{cfg.transport}' ist ungueltig. "
            f"Erlaubt: {', '.join(TRANSPORTS)}"
        )
    if cfg.depth_policy not in DEPTH_POLICIES:
        raise ConfigError(
            f"exchanges.{name}.depth_policy = '{cfg.depth_policy}' ist ungueltig. "
            f"Erlaubt: {', '.join(DEPTH_POLICIES)}"
        )
    if cfg.depth < 1:
        raise ConfigError(f"exchanges.{name}.depth muss >= 1 sein.")
    if not cfg.symbols:
        raise ConfigError(f"exchanges.{name}: keine Symbole konfiguriert.")
    return cfg


def _validate_general(g: GeneralConfig) -> None:
    if not g.symbols:
        raise ConfigError("general.symbols darf nicht leer sein.")
    for sym in g.symbols:
        if "/" not in sym:
            raise ConfigError(
                f"Symbol '{sym}' muss kanonisch als BASE/QUOTE angegeben werden, "
                f"z.B. 'ETH/USDC'."
            )
    if g.interval_ms < 50:
        raise ConfigError("general.interval_ms muss mindestens 50 betragen.")
    if g.depth < 1:
        raise ConfigError("general.depth muss >= 1 sein.")
    if g.transport not in TRANSPORTS:
        raise ConfigError(
            f"general.transport = '{g.transport}' ist ungueltig. "
            f"Erlaubt: {', '.join(TRANSPORTS)}"
        )
    if g.depth_policy not in DEPTH_POLICIES:
        raise ConfigError(
            f"general.depth_policy = '{g.depth_policy}' ist ungueltig. "
            f"Erlaubt: {', '.join(DEPTH_POLICIES)}"
        )


def _validate_storage(s: StorageConfig) -> None:
    if s.backend not in BACKENDS:
        raise ConfigError(
            f"storage.backend = '{s.backend}' wird nicht unterstuetzt. "
            f"Verfuegbar: {', '.join(BACKENDS)}"
        )
    if s.batch_size < 1:
        raise ConfigError("storage.batch_size muss >= 1 sein.")
    if s.queue_maxsize < 100:
        raise ConfigError("storage.queue_maxsize muss >= 100 sein.")


def _validate_connection(c: ConnectionConfig) -> None:
    if c.reconnect_backoff_min_s <= 0:
        raise ConfigError("connection.reconnect_backoff_min_s muss > 0 sein.")
    if c.reconnect_backoff_max_s < c.reconnect_backoff_min_s:
        raise ConfigError(
            "connection.reconnect_backoff_max_s muss >= reconnect_backoff_min_s sein."
        )
    if c.stale_after_s <= 0:
        raise ConfigError("connection.stale_after_s muss > 0 sein.")
    if c.ws_failures_before_rest < 1:
        raise ConfigError("connection.ws_failures_before_rest muss >= 1 sein.")
    if c.rest_fallback_duration_s <= 0:
        raise ConfigError("connection.rest_fallback_duration_s muss > 0 sein.")
