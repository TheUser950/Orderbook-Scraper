"""Writer-Schnittstelle.

Bewusst als Protocol formuliert, damit spaeter ein Postgres-/TimescaleDB-Writer
danebengestellt werden kann, ohne dass Sampler, Supervisor oder Adapter
angefasst werden muessen.
"""

from __future__ import annotations

from typing import Protocol

from ..models import OrderBookSnapshot


class Writer(Protocol):
    async def start(self, config_json: str, config_hash: str, version: str) -> int:
        """Legt das Schema an, registriert den Lauf und liefert die run_id."""
        ...

    def submit_snapshot(self, snap: OrderBookSnapshot) -> None: ...

    def submit_event(
        self,
        exchange: str,
        event: str,
        endpoint: str | None = None,
        detail: str | None = None,
    ) -> None:
        """Verbindungsereignis protokollieren (connect, disconnect, error, ...).

        Landet in der DB, nicht nur im Log: bei der spaeteren Auswertung muss
        nachvollziehbar sein, warum eine Luecke im Datensatz existiert.
        """
        ...

    def submit_latency(
        self,
        exchange: str,
        endpoint: str,
        handshake_ms: float | None,
        first_msg_ms: float | None,
        chosen: bool,
    ) -> None: ...

    async def close(self) -> None:
        """Restliche Zeilen schreiben und Verbindung sauber schliessen."""
        ...
