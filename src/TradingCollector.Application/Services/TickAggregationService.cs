using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradingCollector.Core.Interfaces;
using TradingCollector.Core.Models;

namespace TradingCollector.Application.Services;

public sealed class TickAggregationService : BackgroundService
{
    private const int BatchSize = 100;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan StatsInterval = TimeSpan.FromSeconds(10);

    private readonly IEnumerable<IExchangeClient> _clients;
    private readonly ITickRepository _repository;
    private readonly TickDeduplicator _deduplicator;
    private readonly ILogger<TickAggregationService> _logger;

    private readonly Channel<Tick> _channel = Channel.CreateBounded<Tick>(new BoundedChannelOptions(10_000)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleReader = true,
        SingleWriter = false,
    });

    private long _totalProcessed;
    private long _totalDuplicates;
    private long _totalDropped;

    // Per-source counters: source name → saved tick count
    private readonly ConcurrentDictionary<string, long> _perSourceProcessed = new();
    private readonly ConcurrentDictionary<string, long> _perSourceDuplicates = new();

    public TickAggregationService(
        IEnumerable<IExchangeClient> clients,
        ITickRepository repository,
        TickDeduplicator deduplicator,
        ILogger<TickAggregationService> logger)
    {
        _clients = clients;
        _repository = repository;
        _deduplicator = deduplicator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _repository.InitializeAsync(stoppingToken);

        var tasks = new List<Task>();

        foreach (var client in _clients)
            tasks.Add(Task.Run(() => ProduceAsync(client, stoppingToken), stoppingToken));

        tasks.Add(Task.Run(() => ConsumeAsync(stoppingToken), stoppingToken));
        tasks.Add(Task.Run(() => LogStatsAsync(stoppingToken), stoppingToken));

        await Task.WhenAll(tasks);
    }

    private async Task ProduceAsync(IExchangeClient client, CancellationToken ct)
    {
        _logger.LogInformation("[{Source}] Producer started", client.Name);
        try
        {
            await foreach (var tick in client.StreamAsync(ct))
            {
                if (!_deduplicator.IsNew(tick))
                {
                    Interlocked.Increment(ref _totalDuplicates);
                    _perSourceDuplicates.AddOrUpdate(tick.Source, 1, (_, v) => v + 1);
                    continue;
                }

                if (!_channel.Writer.TryWrite(tick))
                    Interlocked.Increment(ref _totalDropped);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{Source}] Producer failed", client.Name);
        }
        finally
        {
            _logger.LogInformation("[{Source}] Producer stopped", client.Name);
        }
    }

    private async Task ConsumeAsync(CancellationToken ct)
    {
        var batch = new List<Tick>(BatchSize);

        using var timer = new PeriodicTimer(FlushInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                while (_channel.Reader.TryRead(out var tick))
                {
                    batch.Add(tick);
                    if (batch.Count >= BatchSize)
                        await FlushAsync(batch, ct);
                }

                if (batch.Count > 0)
                    await FlushAsync(batch, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer failed");
        }

        // Final drain after cancellation
        while (_channel.Reader.TryRead(out var remaining))
            batch.Add(remaining);

        if (batch.Count > 0)
            await FlushAsync(batch, CancellationToken.None);
    }

    private async Task FlushAsync(List<Tick> batch, CancellationToken ct)
    {
        if (batch.Count == 0)
            return;

        var snapshot = new List<Tick>(batch);
        batch.Clear();

        try
        {
            await _repository.SaveBatchAsync(snapshot, ct);
            Interlocked.Add(ref _totalProcessed, snapshot.Count);

            // Update per-source counters
            foreach (var tick in snapshot)
                _perSourceProcessed.AddOrUpdate(tick.Source, 1, (_, v) => v + 1);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save batch of {Count} ticks", snapshot.Count);
        }
    }

    private async Task LogStatsAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(StatsInterval, ct);

                var total      = Interlocked.Read(ref _totalProcessed);
                var duplicates = Interlocked.Read(ref _totalDuplicates);
                var dropped    = Interlocked.Read(ref _totalDropped);
                var queue      = _channel.Reader.Count;

                _logger.LogInformation(
                    "─── Stats ───  total saved: {Total} | dupes skipped: {Dupes} | dropped: {Dropped} | queue: {Queue}",
                    total, duplicates, dropped, queue);

                foreach (var source in _perSourceProcessed.Keys.OrderBy(s => s))
                {
                    _perSourceProcessed.TryGetValue(source, out var saved);
                    _perSourceDuplicates.TryGetValue(source, out var dupes);
                    _logger.LogInformation(
                        "         [{Source}]  saved: {Saved,6} | dupes: {Dupes,6}",
                        source, saved, dupes);
                }
            }
        }
        catch (OperationCanceledException) { }
    }
}
