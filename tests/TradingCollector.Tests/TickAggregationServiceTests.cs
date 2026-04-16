using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using TradingCollector.Application.Services;
using TradingCollector.Core.Interfaces;
using TradingCollector.Core.Models;

namespace TradingCollector.Tests;

public sealed class TickAggregationServiceTests
{
    private static Tick MakeTick(string ticker, string source, long tsMs = 1_713_271_200_000L) => new()
    {
        Ticker = ticker,
        Price = 45000m,
        Volume = 0.001m,
        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(tsMs),
        Source = source,
    };

    private static async Task RunServiceAsync(
        TickAggregationService service,
        int runForMs = 1_200)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await service.StartAsync(cts.Token);
        await Task.Delay(runForMs, CancellationToken.None);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Ticks_ArePersistedToRepository()
    {
        // Arrange
        var repository = Substitute.For<ITickRepository>();
        var dedup = new TickDeduplicator();
        var logger = NullLogger<TickAggregationService>.Instance;

        var tick = MakeTick("BTCUSDT", "TestExchange");
        var client = Substitute.For<IExchangeClient>();
        client.Name.Returns("TestExchange");
        client.StreamAsync(Arg.Any<CancellationToken>())
              .Returns(AsyncEnumerableHelper.Single(tick));

        var service = new TickAggregationService([client], repository, dedup, logger);

        // Act
        await RunServiceAsync(service);

        // Assert
        await repository.Received(1).InitializeAsync(Arg.Any<CancellationToken>());
        await repository.Received().SaveBatchAsync(
            Arg.Is<IReadOnlyList<Tick>>(list => list.Any(t => t.Ticker == "BTCUSDT" && t.Source == "TestExchange")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DuplicateTick_IsNotPersistedTwice()
    {
        // Arrange
        var allSaved = new List<Tick>();
        var repository = Substitute.For<ITickRepository>();

        // Capture saved ticks BEFORE the service runs (When/Do pattern)
        repository
            .When(r => r.SaveBatchAsync(Arg.Any<IReadOnlyList<Tick>>(), Arg.Any<CancellationToken>()))
            .Do(call => allSaved.AddRange(call.Arg<IReadOnlyList<Tick>>()));

        var dedup = new TickDeduplicator();
        var logger = NullLogger<TickAggregationService>.Instance;

        var tick = MakeTick("BTCUSDT", "TestExchange");
        var client = Substitute.For<IExchangeClient>();
        client.Name.Returns("TestExchange");

        // Emit the same tick twice — second should be deduped
        client.StreamAsync(Arg.Any<CancellationToken>())
              .Returns(new[] { tick, tick }.ToAsyncEnumerable());

        var service = new TickAggregationService([client], repository, dedup, logger);

        // Act
        await RunServiceAsync(service);

        // Assert — only 1 unique tick persisted
        allSaved.Should().HaveCount(1);
        allSaved[0].Ticker.Should().Be("BTCUSDT");
    }

    [Fact]
    public async Task MultipleClients_AllTicksAreCollected()
    {
        // Arrange
        var allSaved = new List<Tick>();
        var repository = Substitute.For<ITickRepository>();

        repository
            .When(r => r.SaveBatchAsync(Arg.Any<IReadOnlyList<Tick>>(), Arg.Any<CancellationToken>()))
            .Do(call => allSaved.AddRange(call.Arg<IReadOnlyList<Tick>>()));

        var dedup = new TickDeduplicator();
        var logger = NullLogger<TickAggregationService>.Instance;

        var clientA = Substitute.For<IExchangeClient>();
        clientA.Name.Returns("ExchangeA");
        clientA.StreamAsync(Arg.Any<CancellationToken>())
               .Returns(AsyncEnumerableHelper.Single(MakeTick("BTCUSDT", "ExchangeA", 1_000L)));

        var clientB = Substitute.For<IExchangeClient>();
        clientB.Name.Returns("ExchangeB");
        clientB.StreamAsync(Arg.Any<CancellationToken>())
               .Returns(AsyncEnumerableHelper.Single(MakeTick("BTCUSDT", "ExchangeB", 2_000L)));

        var service = new TickAggregationService([clientA, clientB], repository, dedup, logger);

        // Act
        await RunServiceAsync(service);

        // Assert — ticks from both exchanges persisted
        allSaved.Should().Contain(t => t.Source == "ExchangeA");
        allSaved.Should().Contain(t => t.Source == "ExchangeB");
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

file static class AsyncEnumerableHelper
{
    public static async IAsyncEnumerable<T> Single<T>(T item)
    {
        await Task.Yield();
        yield return item;
    }
}

file static class EnumerableExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
        {
            await Task.Yield();
            yield return item;
        }
    }
}
