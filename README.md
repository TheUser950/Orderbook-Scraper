# Orderbook Scraper

Collects L2 order book data (top-N bids/asks) from ten crypto exchanges
simultaneously - over WebSocket where possible, with REST polling as a
fallback - and writes it on a fixed grid into a SQLite database for later
research. It runs unattended, tolerates failures (a dead exchange never stops
the process) and records connection events so gaps in the dataset stay
traceable.

Supported exchanges: Binance, OKX, Bybit, Bitget, KuCoin, Gate, HTX, Coinbase,
BingX, MEXC - all over WebSocket, with REST as an automatic safety net.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

On Linux the activation is `source .venv/bin/activate`.

## Configuration

Everything relevant lives in `config.yaml`: symbols (`BASE/QUOTE`, e.g.
`ETH/USDC`), sampling interval, desired order book depth, transport
(`ws` / `rest` / `auto`) and a per-exchange on/off switch. All options are
commented in the file itself.

Important: **not every exchange lists every pair.** This is checked against
each exchange's instrument list at startup (and in `--dry-run`); unlisted
pairs are skipped and logged clearly instead of silently producing empty data.

## Usage

```powershell
# Check config, listings and endpoint latencies - writes nothing
python run.py --dry-run

# Run normally, until Ctrl+C
python run.py

# Time-boxed (e.g. for testing)
python run.py --duration 120

# Different config file
python run.py --config other.yaml
```

Logs go to `logs/scraper.log` (rotating), data to `data/orderbook.db`.

### Checking data quality

```powershell
python tools\inspect_db.py
```

Shows per exchange: row count, gaps in the sampling grid, freshness
(`age_ms`), mean spread, share of `stale`/`crossed`/`partial` flags and the
number of successful connections. This is the real acceptance test - it shows
not just *that* data is flowing, but whether it is usable.

## Architecture (in brief)

Every exchange adapter (`src/obscraper/exchanges/`) keeps a live top-N order
book in memory - no matter whether it is fed by a WS push, WS snapshot+delta,
or REST polling. A central sampler (`sampler.py`) reads that book for **all**
exchanges on every wall-clock tick, which makes `ts_grid` in the database a
direct join key across exchanges. One supervisor per exchange
(`supervisor.py`) keeps the connection alive, reconnects with backoff and
isolates failures: a stalled or crashing exchange never affects the others.

At startup - if `connection.pick_fastest_endpoint: true` - the latency of the
several WS hosts an exchange offers (e.g. Binance's hosts, OKX standard/AWS)
is measured and the fastest one is selected (`latency.py`).

## Robustness against protocol changes

Exchanges change their WebSocket protocols - MEXC shut down its entire JSON
stream in 2025. There are two layers of defence against that:

1. **Additive changes are tolerated.** The MEXC protobuf reader
   (`exchanges/_protobuf.py`) skips unknown fields based on their wire type
   instead of aborting the parse. New fields in the schema do not disturb it.
2. **Real breaking changes do not cause data loss.** If a WebSocket fails
   repeatedly under `transport: auto`
   (`connection.ws_failures_before_rest`), the supervisor switches to REST
   polling for `rest_fallback_duration_s` and then retries the WebSocket. The
   switch is recorded in every row (column `transport`) and in
   `connection_events`, so during analysis it is clear which data arrived over
   which path.

## Tests

```powershell
python tests\test_protobuf.py
```

Covers the protobuf wire-format reader and the MEXC parsing, including the
cases that matter in production: unknown fields, truncated frames, exact price
strings and depth truncation.

## Known limitations

- **MEXC** runs over protobuf (the JSON WebSocket was shut down). It is
  decoded with a minimal, dependency-free wire-format reader instead of
  generated `_pb2.py` files - for three message types that is leaner and
  avoids protoc as a build step. Reference schema:
  [mexcdevelop/websocket-proto](https://github.com/mexcdevelop/websocket-proto).
  If field numbers are renumbered, the REST fallback takes over.
- **OKX/Bitget at greater depth** and **Coinbase** maintain the book locally
  from snapshot + deltas; the checksum supplied by the exchange is currently
  not verified. Good enough for research purposes, but it can be added in
  `exchanges/okx.py` / `exchanges/bitget.py` if needed.
- **KuCoin** fetches its token and WS host dynamically through a REST
  bootstrap (`/bullet-public`); endpoint racing effectively drops out.
- Some exact field names (notably BingX) may change. `parse()` is written
  defensively everywhere - an unexpected message structure yields an empty
  list rather than an exception; the supervisor reconnects and the process
  keeps running.

## Storage sizing

Measured with 5 pairs across 9 active exchanges at `interval_ms: 100` and
`depth: 20`: about **400 rows/s, 1.45 kB per row, ~50 GB per day**.

Roughly 52% of those rows are byte-identical duplicates, because the slower
exchanges (HTX ~1/s, MEXC ~2/s, BingX ~1.7/s) simply do not push any faster.
Ways to reduce the volume:

| Change | Result |
|---|---|
| unchanged | 50 GB/day |
| `depth: 10` | 32 GB/day |
| `interval_ms: 1000` | 5 GB/day |

Measured update rates per exchange (ETH/USDC, 90s sample): Bybit ~31 ms
median, Bitget/Binance ~94 ms, OKX ~109 ms, KuCoin ~125 ms, Gate ~140 ms,
MEXC ~500 ms, BingX ~594 ms, HTX ~1000 ms. Most of these are hard throttles on
the exchange side, not a function of market activity.

## Deployment on your own server

The script is built to run unattended (backoff, watchdog, error isolation,
graceful shutdown on SIGINT/SIGTERM). For continuous operation on a Linux
server a systemd unit is a good fit:

```ini
# /etc/systemd/system/orderbook-scraper.service
[Unit]
Description=Orderbook Scraper
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/orderbook-scraper
ExecStart=/opt/orderbook-scraper/.venv/bin/python run.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now orderbook-scraper
journalctl -u orderbook-scraper -f
```

SQLite (in WAL mode) handles the write rates seen here without trouble. If
needed, the writer protocol in `storage/base.py` can be extended with a
Postgres/TimescaleDB writer without touching the rest of the program.
