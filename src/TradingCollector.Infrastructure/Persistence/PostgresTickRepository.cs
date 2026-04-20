using Npgsql;
using TradingCollector.Core.Interfaces;
using TradingCollector.Core.Models;

namespace TradingCollector.Infrastructure.Persistence;

/// <summary>
/// PostgreSQL repository using Npgsql binary COPY for high-throughput batch inserts.
/// </summary>
public sealed class PostgresTickRepository : ITickRepository
{
    private readonly string _connectionString;

    public PostgresTickRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "schema.sql");
        string sql;

        if (File.Exists(schemaPath))
        {
            sql = await File.ReadAllTextAsync(schemaPath, cancellationToken);
        }
        else
        {
            // Fallback inline DDL so the app works from any working directory
            sql = """
                CREATE TABLE IF NOT EXISTS ticks (
                    id          BIGSERIAL     PRIMARY KEY,
                    ticker      VARCHAR(20)   NOT NULL,
                    price       NUMERIC(20,8) NOT NULL,
                    volume      NUMERIC(20,8) NOT NULL,
                    timestamp   TIMESTAMPTZ   NOT NULL,
                    source      VARCHAR(50)   NOT NULL,
                    received_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ticks_ticker_ts ON ticks (ticker, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_ticks_source    ON ticks (source);
                """;
        }

        await using var conn = await OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SaveBatchAsync(IReadOnlyList<Tick> ticks, CancellationToken cancellationToken = default)
    {
        if (ticks.Count == 0)
            return;

        await using var conn = await OpenConnectionAsync(cancellationToken);

        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY ticks (ticker, price, volume, timestamp, source) FROM STDIN (FORMAT BINARY)",
            cancellationToken);

        foreach (var tick in ticks)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(tick.Ticker, NpgsqlTypes.NpgsqlDbType.Varchar, cancellationToken);
            await writer.WriteAsync(tick.Price, NpgsqlTypes.NpgsqlDbType.Numeric, cancellationToken);
            await writer.WriteAsync(tick.Volume, NpgsqlTypes.NpgsqlDbType.Numeric, cancellationToken);
            await writer.WriteAsync(tick.Timestamp, NpgsqlTypes.NpgsqlDbType.TimestampTz, cancellationToken);
            await writer.WriteAsync(tick.Source, NpgsqlTypes.NpgsqlDbType.Varchar, cancellationToken);
        }

        await writer.CompleteAsync(cancellationToken);
    }

    // Connection is disposed on failure — no native resource leak on OpenAsync exception (#7)
    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var conn = new NpgsqlConnection(_connectionString);
        try
        {
            await conn.OpenAsync(ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }
}
