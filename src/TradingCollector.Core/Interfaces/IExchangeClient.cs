using TradingCollector.Core.Models;

namespace TradingCollector.Core.Interfaces;

public interface IExchangeClient : IAsyncDisposable
{
    string Name { get; }
    IAsyncEnumerable<Tick> StreamAsync(CancellationToken cancellationToken);
}
