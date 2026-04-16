using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TradingCollector.Core.Models;

namespace TradingCollector.Infrastructure.Exchange;

/// <summary>
/// Connects to Bybit V5 public spot trade stream.
/// URL: wss://stream.bybit.com/v5/public/spot
/// After connecting, sends a subscribe frame for publicTrade.{SYMBOL}.
/// Message format:
///   {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1713271200000,
///    "data":[{"T":1713271200000,"s":"BTCUSDT","p":"45000.50","v":"0.001","S":"Buy"}]}
/// </summary>
public class BybitExchangeClient : WebSocketExchangeClientBase
{
    private readonly string _symbol;

    public BybitExchangeClient(ExchangeConfig config, string symbol, ILogger<BybitExchangeClient> logger)
        : base(config, logger)
    {
        _symbol = symbol;
    }

    protected override async Task OnConnectedAsync(ClientWebSocket ws, CancellationToken ct)
    {
        var subscribeMsg = JsonSerializer.Serialize(new
        {
            op = "subscribe",
            args = new[] { $"publicTrade.{_symbol}" },
        });

        var bytes = Encoding.UTF8.GetBytes(subscribeMsg);
        await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct);
        Logger.LogDebug("[{Exchange}] Subscribed to publicTrade.{Symbol}", Name, _symbol);
    }

    protected override Tick? ParseMessage(string message)
    {
        using var doc = JsonDocument.Parse(message);
        var root = doc.RootElement;

        // Confirmation / ping frames have no "data" array
        if (!root.TryGetProperty("topic", out _) || !root.TryGetProperty("data", out var dataArr))
            return null;

        // Bybit sends arrays; take the first trade entry
        if (dataArr.GetArrayLength() == 0)
            return null;

        var entry = dataArr[0];
        var ticker = entry.GetProperty("s").GetString()!;
        var price = decimal.Parse(entry.GetProperty("p").GetString()!,
            System.Globalization.CultureInfo.InvariantCulture);
        var volume = decimal.Parse(entry.GetProperty("v").GetString()!,
            System.Globalization.CultureInfo.InvariantCulture);
        var tsMs = entry.GetProperty("T").GetInt64();

        return new Tick
        {
            Ticker = ticker,
            Price = price,
            Volume = volume,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(tsMs),
            Source = Name,
        };
    }
}
