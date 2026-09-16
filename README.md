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

## Recording modes

`general.mode` decides how data is recorded:

| Mode | Table | What it gives you |
|---|---|---|
| `grid` | `snapshots` | The current book sampled every `interval_ms`. Rows are time-aligned across exchanges, so `ts_grid` is a direct join key. |
| `stream` | `book_updates` | Every incoming update written as it arrives, with its exact arrival time. Needs resampling before exchanges can be compared. |
| `both` | both | Both at once, into their two separate tables. |

Stream mode records a **top-N row per update**, not raw deltas. That is not a
shortcut: only OKX, Bitget, Bybit and Coinbase send true deltas at all —
Binance, HTX, MEXC, Gate, KuCoin and BingX push a complete top-N snapshot on
every message. A raw-delta table could not be filled uniformly. The
`is_snapshot` column records which kind an update was, so you can see when a
locally maintained book was reset.

### Skipping unchanged rows

With `storage.skip_unchanged: true` (the default) a row is only written when
the book actually changed since the last one for that exchange/symbol. This is
information-preserving: a missing row means "same as the previous one", so
forward-fill when analysing.

It matters a lot. At a 100 ms grid the slow exchanges push far less often than
they are sampled — measured over 120 s, dedupe cut the dataset from 47,656 to
18,264 rows, **50.2 GB/day down to 20.9 GB/day**.

Two details make it safe:

- The comparison includes the quality flags, not just the levels. A feed that
  goes stale keeps identical levels but still produces a row — otherwise a dead
  feed would be indistinguishable from a quiet market.
- `storage.heartbeat_s` (default 60) writes at least one row per symbol per
  minute regardless. So any gap longer than that means something was genuinely
  wrong, and forward-fill distance is bounded.

## Executed trades (the tape)

With `general.trades: true` (the default) the scraper also records every
executed trade into the `trades` table. This is independent of `mode`: a trade
is an event that either happened or did not, so there is no grid concept — each
trade is written as it arrives. All ten exchanges publish a public trade feed.

### The aggressor side

`side` is always normalised to the **taker** (aggressor): `buy` means a taker
lifted the offer, `sell` means a taker hit the bid. Exchanges disagree sharply
on how they express this, and getting it wrong does not crash anything — it
silently inverts every order-flow conclusion. `raw_side` therefore keeps
whatever the exchange originally sent, so the normalisation stays auditable.

| Exchange | Source field | Handling |
|---|---|---|
| Binance | `m` (buyer is maker) | inverted → taker |
| Coinbase | `side` = **maker** side | **inverted** → taker |
| OKX, Bybit, Bitget, Gate, HTX, KuCoin | `side` / `direction` | already the taker |
| MEXC | `tradeType` 1/2 | mapped directly |
| BingX | `m` | **not used — see below** |

This is verified empirically, not just from documentation: a taker buy must
print at or above the mid and a taker sell at or below it. Measured against the
contemporaneous book over a 180 s run, nine exchanges agree at 76–99.5%.

**BingX records no side at all.** Its `m` flag failed validation on two
independent tests — no relationship to where trades printed, and a
book-independent tick test at 47.8%, i.e. random. Inverting it does not help.
Rather than write a plausible-looking but unverifiable direction, `side` is left
NULL there and the raw flag is preserved in `raw_side`.

**MEXC uses aggregated trades**, because MEXC refuses the individual-trade
channel outright (`Reason: Blocked!`). Its aggregated feed does carry trade ids.
Aggregation blurs the price signal, which is why MEXC scores lower (76%) than
the rest — expected, not a defect.

### Deduplication

A partial unique index on `(exchange, symbol, trade_id)` deduplicates wherever
an exchange supplies an id, and never drops rows where it does not. That is what
makes the REST fallback safe: overlapping polls re-deliver trades that are
simply ignored. Verified — a 65 s run with a dead WebSocket collected 4,443
trades over REST with zero duplicates despite heavy overlap.

## Configuration

Everything relevant lives in `config.yaml`: symbols (`BASE/QUOTE`, e.g.
`ETH/USDC`), recording mode, sampling interval, desired order book depth,
transport (`ws` / `rest` / `auto`) and a per-exchange on/off switch. All
options are commented in the file itself.

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
python tests\test_dedupe.py
python tests\test_trades.py
```

`test_protobuf.py` covers the protobuf wire-format reader and the MEXC
parsing, including the cases that matter in production: unknown fields,
truncated frames, exact price strings and depth truncation.

`test_dedupe.py` covers the unchanged-row check and the stream hook, pinning
down the cases where a row must be written even though the levels look
identical — above all a feed going stale.

`test_trades.py` pins the aggressor-side convention down with a captured frame
per exchange, including the two inversions and BingX's deliberate NULL.

All three run offline in under a second.

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
- **BingX sends its asks worst-price-first**, unlike every other exchange here
  (verified against the live feed: bids descending as usual, but asks
  descending too, putting the best ask last). `bingx.py` therefore sorts both
  sides explicitly *before* truncating to the configured depth — truncating
  first would keep the worst levels and discard the best. Sorting rather than
  reversing keeps it correct if BingX ever changes the order.
- Some exact field names (notably BingX) may change. `parse()` is written
  defensively everywhere - an unexpected message structure yields an empty
  list rather than an exception; the supervisor reconnects and the process
  keeps running.

## Storage sizing

All figures measured over 120 s with 5 pairs, `depth: 20` and
`interval_ms: 100` (41 active exchange/pair combinations, ~1.45 kB per row):

| Configuration | Rows | Per day |
|---|---|---|
| `grid`, no dedupe | 47,656 | 50.2 GB |
| **`grid` + dedupe (current default)** | **18,264** | **20.9 GB** |
| `stream` + dedupe | 24,202 | 25.1 GB |
| `grid` + dedupe, `depth: 10` | — | ~13 GB |
| **trades** (`trades: true`) | 220/s | **3.8 GB** |

Trades are cheap: ~200 bytes per row against ~1450 for a book row, about 11% of
total storage in a `grid` + `trades` configuration.

Further reductions: `depth: 10` removes roughly 36% of each row (the level
JSON is 72% of it), and `interval_ms: 1000` cuts the grid volume by about 10x.

Measured update rates per exchange (ETH/USDC, 90 s sample): Bybit ~31 ms
median, Bitget/Binance ~94 ms, OKX ~109 ms, KuCoin ~125 ms, Gate ~140 ms,
MEXC ~500 ms, BingX ~594 ms, HTX ~1000 ms. Most of these are hard throttles on
the exchange side, not a function of market activity — which is exactly why
sampling at 100 ms produced so many duplicates before dedupe.

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
