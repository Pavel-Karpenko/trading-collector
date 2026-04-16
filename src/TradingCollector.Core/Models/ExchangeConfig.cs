namespace TradingCollector.Core.Models;

public sealed class ExchangeConfig
{
    public required string Name { get; init; }
    public required string WebSocketUrl { get; init; }
    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(60);
}
