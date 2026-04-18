# Trading Collector

Real-time trading data aggregation system built with **.NET 9**. Connects simultaneously to **Binance**, **Bybit**, and **Kraken** public WebSocket APIs, normalizes ticks to a unified format, deduplicates, and batch-inserts into **PostgreSQL**.

## Features

- Parallel WebSocket connections to 3 exchanges (50–100 ticks/sec)
- Automatic reconnection with exponential backoff (5s → 10s → 20s → 60s max)
- In-memory deduplication with a sliding window of 10 000 keys
- High-throughput batch insert via Npgsql binary COPY
- Per-source statistics logged every 10 seconds
- Clean Architecture: Core → Application → Infrastructure ← Host

## Architecture

```
Binance WS ─┐
Bybit   WS ─┼─► Channel<Tick>(10 000) ─► PeriodicTimer flush ─► Npgsql COPY ─► PostgreSQL
Kraken  WS ─┘
```

Each exchange client runs as an independent producer task. A single consumer drains the channel every 500 ms or whenever a batch of 100 ticks accumulates.

## Quick start

**Requires:** Docker + Docker Compose.

```bash
git clone https://github.com/Pavel-Karpenko/trading-collector.git
cd trading-collector
docker compose up --build
```

That's it. The collector starts after PostgreSQL passes its health check, creates the schema automatically, and begins streaming ticks from all three exchanges.

### Stop

```bash
docker compose down
```

Data is persisted in a named volume (`postgres_data`) and survives restarts.

## Run locally (without Docker)

**Requires:** .NET 9 SDK, a running PostgreSQL instance.

```bash
# 1. Set connection string (or edit src/TradingCollector.Host/appsettings.json)
export ConnectionStrings__Postgres="Host=localhost;Port=5432;Database=trading;Username=trader;Password=secret"

# 2. Run
dotnet run --project src/TradingCollector.Host
```

## Run tests

```bash
dotnet test TradingCollector.sln
```

16 tests covering deduplication logic, per-exchange message parsing, and the full aggregation pipeline.

## Check collected data

```bash
# Row count by exchange
docker compose exec postgres psql -U trader -d trading \
  -c "SELECT source, COUNT(*) FROM ticks GROUP BY source ORDER BY source;"

# Latest 10 ticks
docker compose exec postgres psql -U trader -d trading \
  -c "SELECT ticker, price, volume, timestamp, source FROM ticks ORDER BY received_at DESC LIMIT 10;"
```

## Configuration

All settings are in `src/TradingCollector.Host/appsettings.json` and can be overridden via environment variables using the standard .NET double-underscore convention.

| Environment variable | Default | Description |
|---|---|---|
| `ConnectionStrings__Postgres` | `Host=localhost;...` | PostgreSQL connection string |

Exchange URLs and reconnect delays are configured in `Program.cs` via `ExchangeConfig`:

```csharp
new ExchangeConfig
{
    Name = "Binance",
    WebSocketUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade",
    InitialReconnectDelay = TimeSpan.FromSeconds(5),
    MaxReconnectDelay = TimeSpan.FromSeconds(60),
}
```

## Database schema

```sql
CREATE TABLE ticks (
    id          BIGSERIAL PRIMARY KEY,
    ticker      VARCHAR(20)   NOT NULL,
    price       NUMERIC(20,8) NOT NULL,
    volume      NUMERIC(20,8) NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    source      VARCHAR(50)   NOT NULL,
    received_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
```

Indexes on `(ticker, timestamp DESC)` and `(source)` are created automatically on startup.

## Example log output

```
info: TickAggregationService[0]
      [Binance] Producer started
info: TickAggregationService[0]
      [Bybit] Producer started
info: TickAggregationService[0]
      [Kraken] Producer started
info: TickAggregationService[0]
      ─── Stats ───  total saved: 312 | dupes skipped: 4 | dropped: 0 | queue: 0
info: TickAggregationService[0]
               [Binance]  saved:    148 | dupes:      2
info: TickAggregationService[0]
               [Bybit]    saved:    109 | dupes:      1
info: TickAggregationService[0]
               [Kraken]   saved:     55 | dupes:      1
```

## License

[MIT](LICENSE)
