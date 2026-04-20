namespace TradingCollector.Core.Models;

public sealed record Tick
{
    public required string Ticker { get; init; }
    public required decimal Price { get; init; }
    public required decimal Volume { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required string Source { get; init; }

    // Computed once and cached — avoids repeated allocations in the hot dedup path (#4)
    private string? _dedupKey;
    public string DedupKey => _dedupKey ??=
        $"{Source}:{Ticker}:{Price}:{Volume}:{Timestamp.ToUnixTimeMilliseconds()}";
}
