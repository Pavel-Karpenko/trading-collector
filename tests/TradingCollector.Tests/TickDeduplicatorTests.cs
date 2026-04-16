using FluentAssertions;
using TradingCollector.Application.Services;
using TradingCollector.Core.Models;

namespace TradingCollector.Tests;

public sealed class TickDeduplicatorTests
{
    private static Tick MakeTick(string ticker = "BTCUSDT", decimal price = 45000m,
        decimal volume = 0.001m, long tsMs = 1_713_271_200_000L) => new()
    {
        Ticker = ticker,
        Price = price,
        Volume = volume,
        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(tsMs),
        Source = "Test",
    };

    [Fact]
    public void IsNew_ReturnsTrueForFirstSighting()
    {
        var dedup = new TickDeduplicator();
        var tick = MakeTick();

        dedup.IsNew(tick).Should().BeTrue();
    }

    [Fact]
    public void IsNew_ReturnsFalseForDuplicate()
    {
        var dedup = new TickDeduplicator();
        var tick = MakeTick();

        dedup.IsNew(tick); // first
        dedup.IsNew(tick).Should().BeFalse();
    }

    [Fact]
    public void IsNew_DifferentPriceIsNotDuplicate()
    {
        var dedup = new TickDeduplicator();

        dedup.IsNew(MakeTick(price: 45000m)).Should().BeTrue();
        dedup.IsNew(MakeTick(price: 45001m)).Should().BeTrue();
    }

    [Fact]
    public void IsNew_DifferentTimestampIsNotDuplicate()
    {
        var dedup = new TickDeduplicator();

        dedup.IsNew(MakeTick(tsMs: 1_000L)).Should().BeTrue();
        dedup.IsNew(MakeTick(tsMs: 2_000L)).Should().BeTrue();
    }

    [Fact]
    public void IsNew_WindowEvictsOldEntriesWhenFull()
    {
        const int maxSize = 5;
        var dedup = new TickDeduplicator(maxSize);

        // Fill the window
        for (long i = 0; i < maxSize; i++)
            dedup.IsNew(MakeTick(tsMs: i));

        dedup.Count.Should().Be(maxSize);

        // Adding one more should evict the oldest
        dedup.IsNew(MakeTick(tsMs: maxSize));
        dedup.Count.Should().Be(maxSize);
    }

    [Fact]
    public void IsNew_AfterEviction_OldTickIsSeenAsNew()
    {
        const int maxSize = 3;
        var dedup = new TickDeduplicator(maxSize);

        var old = MakeTick(tsMs: 0L);
        dedup.IsNew(old); // seen at position 0

        // Push it out of the window
        for (long i = 1; i <= maxSize; i++)
            dedup.IsNew(MakeTick(tsMs: i));

        // Now the old tick should be considered new again
        dedup.IsNew(old).Should().BeTrue();
    }

    [Fact]
    public void IsNew_IsThreadSafe()
    {
        var dedup = new TickDeduplicator(10_000);
        var seen = 0;

        Parallel.For(0L, 1000L, i =>
        {
            var tick = MakeTick(tsMs: i);
            if (dedup.IsNew(tick))
                Interlocked.Increment(ref seen);
        });

        seen.Should().Be(1000);
    }
}
