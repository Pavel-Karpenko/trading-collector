using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TradingCollector.Core.Models;

namespace TradingCollector.Infrastructure.Exchange;

/// <summary>
/// Connects to Kraken V2 public trade stream.
/// URL: wss://ws.kraken.com/v2
/// After connecting, sends a subscribe frame.
/// Message format:
///   {"channel":"trade","type":"update","data":[
///     {"symbol":"BTC/USD","side":"buy","price":45000.50,"qty":0.001,
///      "timestamp":"2024-04-16T10:00:00.000000Z"}
///   ]}
/// </summary>
public class KrakenExchangeClient : WebSocketExchangeClientBase
{
    private readonly string _symbol;

    public KrakenExchangeClient(ExchangeConfig config, string symbol, ILogger<KrakenExchangeClient> logger)
        : base(config, logger)
    {
        _symbol = symbol;
    }

    protected override async Task OnConnectedAsync(ClientWebSocket ws, CancellationToken ct)
    {
        var subscribeMsg = JsonSerializer.Serialize(new
        {
            method = "subscribe",
            @params = new
            {
                channel = "trade",
                symbol = new[] { _symbol },
            },
        });

        var bytes = Encoding.UTF8.GetBytes(subscribeMsg);
        await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct);
        Logger.LogDebug("[{Exchange}] Subscribed to trade channel for {Symbol}", Name, _symbol);
    }

    protected override Tick? ParseMessage(string message)
    {
        using var doc = JsonDocument.Parse(message);
        var root = doc.RootElement;

        if (!root.TryGetProperty("channel", out var channel) || channel.GetString() != "trade")
            return null;

        if (!root.TryGetProperty("data", out var dataArr) || dataArr.GetArrayLength() == 0)
            return null;

        var entry = dataArr[0];

        var symbol = entry.GetProperty("symbol").GetString()!;
        // Normalize "BTC/USD" → "BTCUSD"
        var ticker = symbol.Replace("/", string.Empty);

        var price = entry.GetProperty("price").GetDecimal();
        var volume = entry.GetProperty("qty").GetDecimal();

        // Kraken returns ISO 8601 timestamp string
        var timestampStr = entry.GetProperty("timestamp").GetString()!;
        var timestamp = DateTimeOffset.Parse(timestampStr, null, System.Globalization.DateTimeStyles.RoundtripKind);

        return new Tick
        {
            Ticker = ticker,
            Price = price,
            Volume = volume,
            Timestamp = timestamp,
            Source = Name,
        };
    }
}
