# Orderbook-Scraper

Sammelt L2-Orderbook-Daten (Top-N bids/asks) von zehn Krypto-Boersen
gleichzeitig - per WebSocket wo moeglich, per REST-Polling als Fallback -
und schreibt sie getaktet in eine SQLite-Datenbank fuer die spaetere
Forschung. Laeuft eigenstaendig, ist fehlertolerant (eine tote Boerse
beendet nie den Prozess) und protokolliert Verbindungsereignisse mit, damit
Luecken im Datensatz spaeter nachvollziehbar sind.

Unterstuetzte Boersen: Binance, OKX, Bybit, Bitget, KuCoin, Gate, HTX,
Coinbase, BingX, MEXC - alle per WebSocket, mit REST als automatischem
Rettungsanker.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Konfiguration

Alles Wesentliche steht in `config.yaml`: Symbole (`BASE/QUOTE`, z.B.
`ETH/USDC`), Sampling-Intervall, gewuenschte Orderbook-Tiefe, Transport
(`ws` / `rest` / `auto`) und pro Boerse aktivierbar/deaktivierbar. Details
und alle Optionen sind in der Datei selbst kommentiert.

Wichtig: **Nicht jede Boerse listet jedes Paar.** Beim Start (und im
`--dry-run`) wird das gegen die Instrumentenliste jeder Boerse geprueft;
nicht gelistete Paare werden uebersprungen und deutlich geloggt statt
still leere Daten zu erzeugen.

## Benutzung

```powershell
# Config, Listings und Endpoint-Latenzen pruefen - schreibt nichts
python run.py --dry-run

# Normal laufen lassen, bis Ctrl+C
python run.py

# Zeitlich begrenzt (z.B. fuer Tests)
python run.py --duration 120

# Andere Config-Datei
python run.py --config other.yaml
```

Logs landen in `logs/scraper.log` (rotierend), Daten in `data/orderbook.db`.

### Datenqualitaet pruefen

```powershell
python tools\inspect_db.py
```

Zeigt je Boerse: Zeilenzahl, Luecken im Sampling-Raster, Frische
(`age_ms`), mittleren Spread, Anteil `stale`/`crossed`/`partial`-Flags und
Anzahl erfolgreicher Verbindungen. Das ist der eigentliche Abnahmetest -
er zeigt nicht nur *dass* Daten fliessen, sondern ob sie brauchbar sind.

## Architektur (kurz)

Jeder Boersen-Adapter (`src/obscraper/exchanges/`) haelt ein lebendes
Top-N-Orderbook im Speicher - egal ob per WS-Push, WS-Snapshot+Delta oder
REST-Polling gefuettert. Ein zentraler Sampler (`sampler.py`) greift dieses
Buch bei jedem Wall-Clock-Tick fuer **alle** Boersen gleichzeitig ab -
dadurch ist `ts_grid` in der Datenbank ein direkter Join-Key ueber Boersen
hinweg. Ein Supervisor pro Boerse (`supervisor.py`) haelt die Verbindung am
Leben, reconnectet mit Backoff und isoliert Fehler: eine haengende oder
abstuerzende Boerse beeinflusst nie die anderen.

Beim Start wird - falls `connection.pick_fastest_endpoint: true` - unter
mehreren WS-Hosts einer Boerse (z.B. Binance' vier Hosts, OKX Standard/AWS)
die Latenz gemessen und der schnellste gewaehlt (`latency.py`).

## Robustheit gegenueber Protokollaenderungen

Boersen aendern ihre WebSocket-Protokolle - MEXC hat 2025 den kompletten
JSON-Stream abgeschaltet. Dagegen gibt es hier zwei Ebenen:

1. **Additive Aenderungen werden toleriert.** Der MEXC-Protobuf-Leser
   (`exchanges/_protobuf.py`) ueberspringt unbekannte Felder anhand ihres
   Wire-Typs, statt das Parsen abzubrechen. Neue Felder im Schema stoeren
   also nicht.
2. **Echte Breaking Changes fuehren nicht zum Datenverlust.** Scheitert ein
   WebSocket bei `transport: auto` mehrfach hintereinander
   (`connection.ws_failures_before_rest`), schaltet der Supervisor
   automatisch fuer `rest_fallback_duration_s` auf REST-Polling um und
   probiert danach erneut den WebSocket. Der Wechsel steht in jeder Zeile
   (Spalte `transport`) und in `connection_events` - bei der Auswertung ist
   also nachvollziehbar, welche Daten ueber welchen Weg kamen.

## Tests

```powershell
python tests\test_protobuf.py
```

Prueft den Protobuf-Wire-Format-Leser und das MEXC-Parsing, inklusive der
Faelle, die im Betrieb zaehlen: unbekannte Felder, abgeschnittene Frames,
exakte Preis-Strings und Tiefenbegrenzung.

## Bekannte Einschraenkungen

- **MEXC** laeuft ueber Protobuf (der JSON-WebSocket wurde abgeschaltet).
  Dekodiert wird ueber einen minimalen, abhaengigkeitsfreien Wire-Format-Leser
  statt ueber generierte `_pb2.py`-Dateien - fuer drei Message-Typen ist das
  schlanker und spart protoc als Build-Schritt. Referenz-Schema:
  [mexcdevelop/websocket-proto](https://github.com/mexcdevelop/websocket-proto).
  Werden Feldnummern umnummeriert, greift der REST-Fallback.
- **OKX/Bitget mit grosser Tiefe** und **Coinbase** pflegen das Buch lokal
  aus Snapshot+Delta; die von der Boerse mitgelieferte Checksumme wird
  aktuell nicht verifiziert. Fuer Forschungszwecke ausreichend, aber bei
  Bedarf in `exchanges/okx.py` / `exchanges/bitget.py` nachruestbar.
- **KuCoin** holt Token und WS-Host dynamisch per REST-Bootstrap
  (`/bullet-public`); Endpoint-Racing entfaellt dadurch faktisch.
- Manche exakten Feldnamen (v.a. BingX) koennen sich aendern. `parse()`
  ist ueberall defensiv geschrieben - eine unerwartete Nachrichtenstruktur
  fuehrt zu einer leeren Liste statt einer Exception; der Supervisor
  reconnectet, der Prozess laeuft weiter.

## Deployment auf dem eigenen Server

Das Skript ist so gebaut, dass es unbeaufsichtigt laeuft (Backoff, Watchdog,
Fehler-Isolation, Graceful Shutdown auf SIGINT/SIGTERM). Fuer den
Dauerbetrieb auf einem Linux-Server bietet sich eine systemd-Unit an:

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

SQLite (WAL-Modus) traegt einen Dauerbetrieb mit den hier anfallenden
Schreibraten problemlos. Bei Bedarf laesst sich `storage/base.py`s
Writer-Protocol um einen Postgres/TimescaleDB-Writer erweitern, ohne den
Rest des Programms anzufassen.
