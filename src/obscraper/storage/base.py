"""Writer interface.

Deliberately expressed as a Protocol so a Postgres/TimescaleDB writer can be
added alongside later without touching the sampler, supervisor or adapters.
"""

from __future__ import annotations

from typing import Protocol

from ..models import OrderBookSnapshot


class Writer(Protocol):
    async def start(self, config_json: str, config_hash: str, version: str) -> int:
        """Create the schema, register the run and return the run_id."""
        ...

    def submit_snapshot(self, snap: OrderBookSnapshot) -> None: ...

    def submit_event(
        self,
        exchange: str,
        event: str,
        endpoint: str | None = None,
        detail: str | None = None,
    ) -> None:
        """Record a connection event (connect, disconnect, error, ...).

        This goes into the database, not just the log: when analysing the data
        later it must be possible to tell why a gap in the dataset exists.
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
        """Write out the remaining rows and close the connection cleanly."""
        ...
