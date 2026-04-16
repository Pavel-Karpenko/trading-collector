using TradingCollector.Core.Models;

namespace TradingCollector.Core.Interfaces;

public interface ITickRepository
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task SaveBatchAsync(IReadOnlyList<Tick> ticks, CancellationToken cancellationToken = default);
}
