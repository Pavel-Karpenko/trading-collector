using TradingCollector.Core.Models;

namespace TradingCollector.Application.Services;

/// <summary>
/// Thread-safe LRU-like deduplicator based on a sliding window of recent tick keys.
/// Uses a single lock + plain Dictionary — consistent, no mixed synchronisation models.
/// </summary>
public sealed class TickDeduplicator
{
    private readonly int _maxSize;
    private readonly Dictionary<string, byte> _seen = new();
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
        lock (_lock)
        {
            if (_seen.ContainsKey(key))
                return false;

            _seen.Add(key, 0);
            _evictionQueue.Enqueue(key);

            while (_evictionQueue.Count > _maxSize)
            {
                if (_evictionQueue.TryDequeue(out var old))
                    _seen.Remove(old);
            }
        }
        return true;
    }

    public int Count { get { lock (_lock) return _seen.Count; } }
}
