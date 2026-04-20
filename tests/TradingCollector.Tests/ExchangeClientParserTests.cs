using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using TradingCollector.Core.Models;
using TradingCollector.Infrastructure.Exchange;

namespace TradingCollector.Tests;

/// <summary>
/// Tests parsing logic of each exchange client by invoking the protected ParseMessages via subclass.
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

        var ticks = client.Parse(json).ToList();

        ticks.Should().ContainSingle();
        ticks[0].Ticker.Should().Be("BTCUSDT");
        ticks[0].Price.Should().Be(45000.50m);
        ticks[0].Volume.Should().Be(0.001m);
        ticks[0].Timestamp.ToUnixTimeMilliseconds().Should().Be(1713271200000L);
        ticks[0].Source.Should().Be("Binance");
    }

    [Fact]
    public void Binance_SkipsNonTradeMessages()
    {
        var client = new TestableBinanceClient();
        const string json = """{"e":"24hrTicker","s":"BTCUSDT","c":"45000.50"}""";

        client.Parse(json).Should().BeEmpty();
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

        var ticks = client.Parse(json).ToList();

        ticks.Should().ContainSingle();
        ticks[0].Ticker.Should().Be("BTCUSDT");
        ticks[0].Price.Should().Be(45000.50m);
        ticks[0].Volume.Should().Be(0.001m);
        ticks[0].Source.Should().Be("Bybit");
    }

    [Fact]
    public void Bybit_ParsesBatchMessage()
    {
        var client = new TestableBybitClient();

        const string json = """
            {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1713271200000,
             "data":[
               {"T":1000,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"45000.00","L":"PlusTick","i":"a","BT":false},
               {"T":2000,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"45001.00","L":"MinusTick","i":"b","BT":false}
             ]}
            """;

        var ticks = client.Parse(json).ToList();

        ticks.Should().HaveCount(2);
        ticks[0].Timestamp.ToUnixTimeMilliseconds().Should().Be(1000L);
        ticks[1].Timestamp.ToUnixTimeMilliseconds().Should().Be(2000L);
    }

    [Fact]
    public void Bybit_SkipsMessagesWithoutData()
    {
        var client = new TestableBybitClient();
        const string json = """{"op":"subscribe","success":true}""";

        client.Parse(json).Should().BeEmpty();
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

        var ticks = client.Parse(json).ToList();

        ticks.Should().ContainSingle();
        ticks[0].Ticker.Should().Be("BTCUSD");
        ticks[0].Price.Should().Be(45000.50m);
        ticks[0].Volume.Should().Be(0.001m);
        ticks[0].Source.Should().Be("Kraken");
    }

    [Fact]
    public void Kraken_ParsesBatchMessage()
    {
        var client = new TestableKrakenClient();

        const string json = """
            {"channel":"trade","type":"update","data":[
              {"symbol":"BTC/USD","side":"buy","price":45000.00,"qty":0.001,"ord_type":"market","trade_id":1,"timestamp":"2024-04-16T10:00:00.000000Z"},
              {"symbol":"BTC/USD","side":"sell","price":45001.00,"qty":0.002,"ord_type":"market","trade_id":2,"timestamp":"2024-04-16T10:00:01.000000Z"}
            ]}
            """;

        var ticks = client.Parse(json).ToList();

        ticks.Should().HaveCount(2);
        ticks[0].Price.Should().Be(45000.00m);
        ticks[1].Price.Should().Be(45001.00m);
    }

    [Fact]
    public void Kraken_SkipsNonTradeChannel()
    {
        var client = new TestableKrakenClient();
        const string json = """{"channel":"heartbeat","type":"heartbeat"}""";

        client.Parse(json).Should().BeEmpty();
    }

    // ── Test helpers (expose protected ParseMessages) ─────────────────────────

    private sealed class TestableBinanceClient : BinanceExchangeClient
    {
        public TestableBinanceClient() : base(
            new ExchangeConfig { Name = "Binance", WebSocketUrl = "wss://unused" },
            NullLogger<BinanceExchangeClient>.Instance) { }

        public IEnumerable<Tick> Parse(string msg) => ParseMessages(msg);
    }

    private sealed class TestableBybitClient : BybitExchangeClient
    {
        public TestableBybitClient() : base(
            new ExchangeConfig { Name = "Bybit", WebSocketUrl = "wss://unused" },
            "BTCUSDT",
            NullLogger<BybitExchangeClient>.Instance) { }

        public IEnumerable<Tick> Parse(string msg) => ParseMessages(msg);
    }

    private sealed class TestableKrakenClient : KrakenExchangeClient
    {
        public TestableKrakenClient() : base(
            new ExchangeConfig { Name = "Kraken", WebSocketUrl = "wss://unused" },
            "BTC/USD",
            NullLogger<KrakenExchangeClient>.Instance) { }

        public IEnumerable<Tick> Parse(string msg) => ParseMessages(msg);
    }
}
