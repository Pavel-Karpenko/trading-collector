using System.Collections.Concurrent;
using TradingCollector.Core.Models;

namespace TradingCollector.Application.Services;

/// <summary>
/// Thread-safe LRU-like deduplicator based on a sliding window of recent tick keys.
/// </summary>
public sealed class TickDeduplicator
{
    private readonly int _maxSize;
    private readonly ConcurrentDictionary<string, byte> _seen = new();
    private readonly Queue<string> _evictionQueue = new();
    private readonly object _lock = new();

    public TickDeduplicator(int maxSize = 10_000)
    {
        _maxSize = maxSize;
    }

    /// <summary>Returns true if the tick has not been seen before and records it.</summary>
    public bool IsNew(Tick tick)
    {
        var key = tick.DedupKey;

        if (_seen.ContainsKey(key))
            return false;

        lock (_lock)
        {
            if (_seen.ContainsKey(key))
                return false;

            _seen.TryAdd(key, 0);
            _evictionQueue.Enqueue(key);

            while (_evictionQueue.Count > _maxSize)
            {
                if (_evictionQueue.TryDequeue(out var old))
                    _seen.TryRemove(old, out _);
            }
        }

        return true;
    }

    public int Count => _seen.Count;
}
