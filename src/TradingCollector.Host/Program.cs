using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradingCollector.Application.Services;
using TradingCollector.Core.Interfaces;
using TradingCollector.Core.Models;
using TradingCollector.Infrastructure.Exchange;
using TradingCollector.Infrastructure.Persistence;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        var connStr = ctx.Configuration.GetSection("ConnectionStrings")["Postgres"]
            ?? throw new InvalidOperationException("ConnectionStrings:Postgres is not configured");

        // Repository
        services.AddSingleton<ITickRepository>(_ => new PostgresTickRepository(connStr));

        // Deduplicator
        services.AddSingleton<TickDeduplicator>();

        // Exchange clients — registered as IExchangeClient (multiple)
        services.AddSingleton<IExchangeClient>(sp => new BinanceExchangeClient(
            new ExchangeConfig
            {
                Name = "Binance",
                WebSocketUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade",
            },
            sp.GetRequiredService<ILogger<BinanceExchangeClient>>()));

        services.AddSingleton<IExchangeClient>(sp => new BybitExchangeClient(
            new ExchangeConfig
            {
                Name = "Bybit",
                WebSocketUrl = "wss://stream.bybit.com/v5/public/spot",
            },
            symbol: "BTCUSDT",
            sp.GetRequiredService<ILogger<BybitExchangeClient>>()));

        services.AddSingleton<IExchangeClient>(sp => new KrakenExchangeClient(
            new ExchangeConfig
            {
                Name = "Kraken",
                WebSocketUrl = "wss://ws.kraken.com/v2",
            },
            symbol: "BTC/USD",
            sp.GetRequiredService<ILogger<KrakenExchangeClient>>()));

        // Main pipeline
        services.AddHostedService<TickAggregationService>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .Build();

await host.RunAsync();
