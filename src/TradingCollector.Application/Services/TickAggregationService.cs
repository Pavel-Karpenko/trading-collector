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

    // Pre-allocated batch buffers — swapped on each flush to avoid per-flush list allocations (#8)
    private List<Tick> _batch = new(BatchSize);
    private List<Tick> _standby = new(BatchSize);

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

        // I/O-bound async methods don't need Task.Run (#10)
        foreach (var client in _clients)
            tasks.Add(ProduceAsync(client, stoppingToken));

        tasks.Add(ConsumeAsync(stoppingToken));
        tasks.Add(LogStatsAsync(stoppingToken));

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
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Event-driven: react immediately when data arrives, or wake after flush interval (#6)
                await Task.WhenAny(
                    _channel.Reader.WaitToReadAsync(ct).AsTask(),
                    Task.Delay(FlushInterval, ct));

                while (_channel.Reader.TryRead(out var tick))
                {
                    _batch.Add(tick);
                    if (_batch.Count >= BatchSize)
                        await FlushAsync(ct);
                }

                if (_batch.Count > 0)
                    await FlushAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer failed");
        }

        // Final drain after cancellation
        while (_channel.Reader.TryRead(out var remaining))
            _batch.Add(remaining);

        if (_batch.Count > 0)
            await FlushAsync(CancellationToken.None);
    }

    private async Task FlushAsync(CancellationToken ct)
    {
        if (_batch.Count == 0)
            return;

        // Swap buffers: consumer continues filling the empty standby while we persist (#8)
        (_batch, _standby) = (_standby, _batch);
        var toSave = _standby;

        try
        {
            // Data preserved until save succeeds — not discarded before the attempt (#2)
            await _repository.SaveBatchAsync(toSave, ct);
            Interlocked.Add(ref _totalProcessed, toSave.Count);

            foreach (var tick in toSave)
                _perSourceProcessed.AddOrUpdate(tick.Source, 1, (_, v) => v + 1);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save batch of {Count} ticks", toSave.Count);
        }
        finally
        {
            toSave.Clear(); // cleared after save attempt, not before (#2)
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
