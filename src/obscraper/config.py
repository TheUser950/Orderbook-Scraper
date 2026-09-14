"""Loading and validating config.yaml.

A bad configuration should abort at startup with a comprehensible message -
not three hours later with a KeyError deep inside an adapter.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

TRANSPORTS = ("ws", "rest", "auto")
DEPTH_POLICIES = ("at_least", "at_most", "nearest")
BACKENDS = ("sqlite",)


class ConfigError(Exception):
    """Raised with a human-readable message."""


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
    # transport=auto only: after this many consecutive WS failures, fall back
    # to REST polling for a while.
    ws_failures_before_rest: int = 3
    rest_fallback_duration_s: float = 600.0
    stale_after_s: float = 15.0
    record_stale_snapshots: bool = True
    ws_ping_interval_s: float = 20.0
    rest_timeout_s: float = 10.0
    health_report_interval_s: float = 60.0


@dataclass(slots=True)
class ExchangeConfig:
    """Resolved settings for exactly one exchange.

    All fields are already filled in from the general section so adapters
    never have to consult two places.
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
        """(json, sha256) - stored per run in the runs table."""
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
    known = set(cls.__slots__)
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"Unknown keys in '{section}': {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(sorted(known))}"
        )
    return cls(**raw)


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"Configuration file not found: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"config.yaml is not valid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError("config.yaml must be a mapping at the top level.")

    unknown_top = set(raw) - {"general", "storage", "connection", "exchanges"}
    if unknown_top:
        raise ConfigError(
            f"Unknown sections in config.yaml: {', '.join(sorted(unknown_top))}"
        )

    general = _coerce("general", raw.get("general") or {}, GeneralConfig)
    storage = _coerce("storage", raw.get("storage") or {}, StorageConfig)
    connection = _coerce("connection", raw.get("connection") or {}, ConnectionConfig)

    _validate_general(general)
    _validate_storage(storage)
    _validate_connection(connection)

    exchanges_raw = raw.get("exchanges") or {}
    if not isinstance(exchanges_raw, dict) or not exchanges_raw:
        raise ConfigError("Section 'exchanges' is missing or empty.")

    exchanges: dict[str, ExchangeConfig] = {}
    for name, over in exchanges_raw.items():
        exchanges[name] = _build_exchange(name, over or {}, general)

    if not any(e.enabled for e in exchanges.values()):
        raise ConfigError("Not a single exchange is enabled (enabled: true).")

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
            f"exchanges.{name} must be a mapping (e.g. '{{ enabled: true }}')."
        )
    unknown = set(over) - allowed
    if unknown:
        raise ConfigError(
            f"Unknown keys under exchanges.{name}: {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(sorted(allowed))}"
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
            f"exchanges.{name}.transport = '{cfg.transport}' is invalid. "
            f"Allowed: {', '.join(TRANSPORTS)}"
        )
    if cfg.depth_policy not in DEPTH_POLICIES:
        raise ConfigError(
            f"exchanges.{name}.depth_policy = '{cfg.depth_policy}' is invalid. "
            f"Allowed: {', '.join(DEPTH_POLICIES)}"
        )
    if cfg.depth < 1:
        raise ConfigError(f"exchanges.{name}.depth must be >= 1.")
    if not cfg.symbols:
        raise ConfigError(f"exchanges.{name}: no symbols configured.")
    return cfg


def _validate_general(g: GeneralConfig) -> None:
    if not g.symbols:
        raise ConfigError("general.symbols must not be empty.")
    for sym in g.symbols:
        if "/" not in sym:
            raise ConfigError(
                f"Symbol '{sym}' must be given in canonical BASE/QUOTE form, "
                f"e.g. 'ETH/USDC'."
            )
    if g.interval_ms < 50:
        raise ConfigError("general.interval_ms must be at least 50.")
    if g.depth < 1:
        raise ConfigError("general.depth must be >= 1.")
    if g.transport not in TRANSPORTS:
        raise ConfigError(
            f"general.transport = '{g.transport}' is invalid. "
            f"Allowed: {', '.join(TRANSPORTS)}"
        )
    if g.depth_policy not in DEPTH_POLICIES:
        raise ConfigError(
            f"general.depth_policy = '{g.depth_policy}' is invalid. "
            f"Allowed: {', '.join(DEPTH_POLICIES)}"
        )


def _validate_storage(s: StorageConfig) -> None:
    if s.backend not in BACKENDS:
        raise ConfigError(
            f"storage.backend = '{s.backend}' is not supported. "
            f"Available: {', '.join(BACKENDS)}"
        )
    if s.batch_size < 1:
        raise ConfigError("storage.batch_size must be >= 1.")
    if s.queue_maxsize < 100:
        raise ConfigError("storage.queue_maxsize must be >= 100.")


def _validate_connection(c: ConnectionConfig) -> None:
    if c.reconnect_backoff_min_s <= 0:
        raise ConfigError("connection.reconnect_backoff_min_s must be > 0.")
    if c.reconnect_backoff_max_s < c.reconnect_backoff_min_s:
        raise ConfigError(
            "connection.reconnect_backoff_max_s must be >= reconnect_backoff_min_s."
        )
    if c.stale_after_s <= 0:
        raise ConfigError("connection.stale_after_s must be > 0.")
    if c.ws_failures_before_rest < 1:
        raise ConfigError("connection.ws_failures_before_rest must be >= 1.")
    if c.rest_fallback_duration_s <= 0:
        raise ConfigError("connection.rest_fallback_duration_s must be > 0.")
