using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using TradingCollector.Core.Models;
using TradingCollector.Infrastructure.Exchange;

namespace TradingCollector.Tests;

/// <summary>
/// Tests parsing logic of each exchange client by invoking the protected ParseMessage via subclass.
/// </summary>
public sealed class ExchangeClientParserTests
{
    // ── Binance ──────────────────────────────────────────────────────────────

    [Fact]
    public void Binance_ParsesTradeTick()
    {
        var client = new TestableBinanceClient();

        const string json = """
            {"e":"trade","E":1713271200001,"s":"BTCUSDT","t":1,"p":"45000.50","q":"0.001",
             "b":1,"a":2,"T":1713271200000,"m":false,"M":true}
            """;

        var tick = client.Parse(json);

        tick.Should().NotBeNull();
        tick!.Ticker.Should().Be("BTCUSDT");
        tick.Price.Should().Be(45000.50m);
        tick.Volume.Should().Be(0.001m);
        tick.Timestamp.ToUnixTimeMilliseconds().Should().Be(1713271200000L);
        tick.Source.Should().Be("Binance");
    }

    [Fact]
    public void Binance_SkipsNonTradeMessages()
    {
        var client = new TestableBinanceClient();
        const string json = """{"e":"24hrTicker","s":"BTCUSDT","c":"45000.50"}""";

        client.Parse(json).Should().BeNull();
    }

    // ── Bybit ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Bybit_ParsesTradeSnapshot()
    {
        var client = new TestableBybitClient();

        const string json = """
            {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1713271200000,
             "data":[{"T":1713271200000,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"45000.50","L":"PlusTick","i":"abc","BT":false}]}
            """;

        var tick = client.Parse(json);

        tick.Should().NotBeNull();
        tick!.Ticker.Should().Be("BTCUSDT");
        tick.Price.Should().Be(45000.50m);
        tick.Volume.Should().Be(0.001m);
        tick.Source.Should().Be("Bybit");
    }

    [Fact]
    public void Bybit_SkipsMessagesWithoutData()
    {
        var client = new TestableBybitClient();
        const string json = """{"op":"subscribe","success":true}""";

        client.Parse(json).Should().BeNull();
    }

    // ── Kraken ────────────────────────────────────────────────────────────────

    [Fact]
    public void Kraken_ParsesTradeUpdate()
    {
        var client = new TestableKrakenClient();

        const string json = """
            {"channel":"trade","type":"update","data":[
              {"symbol":"BTC/USD","side":"buy","price":45000.50,"qty":0.001,
               "ord_type":"market","trade_id":123,"timestamp":"2024-04-16T10:00:00.000000Z"}
            ]}
            """;

        var tick = client.Parse(json);

        tick.Should().NotBeNull();
        tick!.Ticker.Should().Be("BTCUSD");
        tick.Price.Should().Be(45000.50m);
        tick.Volume.Should().Be(0.001m);
        tick.Source.Should().Be("Kraken");
    }

    [Fact]
    public void Kraken_SkipsNonTradeChannel()
    {
        var client = new TestableKrakenClient();
        const string json = """{"channel":"heartbeat","type":"heartbeat"}""";

        client.Parse(json).Should().BeNull();
    }

    // ── Test helpers (expose protected ParseMessage) ──────────────────────────

    private sealed class TestableBinanceClient : BinanceExchangeClient
    {
        public TestableBinanceClient() : base(
            new ExchangeConfig { Name = "Binance", WebSocketUrl = "wss://unused" },
            NullLogger<BinanceExchangeClient>.Instance) { }

        public Tick? Parse(string msg) => ParseMessage(msg);
    }

    private sealed class TestableBybitClient : BybitExchangeClient
    {
        public TestableBybitClient() : base(
            new ExchangeConfig { Name = "Bybit", WebSocketUrl = "wss://unused" },
            "BTCUSDT",
            NullLogger<BybitExchangeClient>.Instance) { }

        public Tick? Parse(string msg) => ParseMessage(msg);
    }

    private sealed class TestableKrakenClient : KrakenExchangeClient
    {
        public TestableKrakenClient() : base(
            new ExchangeConfig { Name = "Kraken", WebSocketUrl = "wss://unused" },
            "BTC/USD",
            NullLogger<KrakenExchangeClient>.Instance) { }

        public Tick? Parse(string msg) => ParseMessage(msg);
    }
}
