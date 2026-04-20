using System.Text.Json;
using Microsoft.Extensions.Logging;
using TradingCollector.Core.Models;

namespace TradingCollector.Infrastructure.Exchange;

/// <summary>
/// Connects to Binance public trade stream.
/// URL: wss://stream.binance.com:9443/ws/{symbol}@trade
/// Message format: {"e":"trade","s":"BTCUSDT","p":"45000.50","q":"0.001","T":1713271200000}
/// Binance sends one trade per message — yields a single tick.
/// </summary>
public class BinanceExchangeClient : WebSocketExchangeClientBase
{
    public BinanceExchangeClient(ExchangeConfig config, ILogger<BinanceExchangeClient> logger)
        : base(config, logger) { }

    protected override IEnumerable<Tick> ParseMessages(string message)
    {
        using var doc = JsonDocument.Parse(message);
        var root = doc.RootElement;

        // Skip non-trade events (e.g. ping/pong, subscription confirmations)
        if (!root.TryGetProperty("e", out var eventType) || eventType.GetString() != "trade")
            yield break;

        var ticker = root.GetProperty("s").GetString()!;
        var price = decimal.Parse(root.GetProperty("p").GetString()!,
            System.Globalization.CultureInfo.InvariantCulture);
        var volume = decimal.Parse(root.GetProperty("q").GetString()!,
            System.Globalization.CultureInfo.InvariantCulture);
        var tsMs = root.GetProperty("T").GetInt64();

        yield return new Tick
        {
            Ticker = ticker,
            Price = price,
            Volume = volume,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(tsMs),
            Source = Name,
        };
    }
}
