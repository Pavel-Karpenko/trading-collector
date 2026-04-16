using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using TradingCollector.Core.Interfaces;
using TradingCollector.Core.Models;

namespace TradingCollector.Infrastructure.Exchange;

/// <summary>
/// Base WebSocket exchange client with automatic reconnection and exponential backoff.
/// Subclasses implement <see cref="ParseMessage"/> and optionally <see cref="OnConnectedAsync"/>.
///
/// The stream is implemented via an internal Channel to avoid C# CS1626
/// (yield is not allowed inside try/catch blocks).
/// </summary>
public abstract class WebSocketExchangeClientBase : IExchangeClient
{
    private readonly ExchangeConfig _config;
    protected readonly ILogger Logger;
    protected WebSocketExchangeClientBase(ExchangeConfig config, ILogger logger)
    {
        _config = config;
        Logger = logger;
    }

    public string Name => _config.Name;

    public async IAsyncEnumerable<Tick> StreamAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Channel separates the reconnect/receive logic (with try/catch) from the yield return
        var channel = Channel.CreateBounded<Tick>(new BoundedChannelOptions(2_000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });

        var producer = Task.Run(() => RunProducerAsync(channel.Writer, cancellationToken), cancellationToken);

        try
        {
            await foreach (var tick in channel.Reader.ReadAllAsync(cancellationToken))
                yield return tick;
        }
        finally
        {
            // Ensure producer completes even if the consumer stops early
            await producer.ConfigureAwait(false);
        }
    }

    private async Task RunProducerAsync(ChannelWriter<Tick> writer, CancellationToken ct)
    {
        var delay = _config.InitialReconnectDelay;
        var attempt = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                using var ws = new ClientWebSocket();
                var connected = false;

                try
                {
                    Logger.LogInformation("[{Exchange}] Connecting (attempt {Attempt}) to {Url}",
                        Name, ++attempt, _config.WebSocketUrl);

                    await ws.ConnectAsync(new Uri(_config.WebSocketUrl), ct);
                    connected = true;
                    Logger.LogInformation("[{Exchange}] Connected", Name);

                    delay = _config.InitialReconnectDelay;
                    attempt = 0;

                    await OnConnectedAsync(ws, ct);
                    await ReceiveAndWriteAsync(ws, writer, ct);
                    Logger.LogInformation("[{Exchange}] Stream ended, reconnecting", Name);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (connected)
                        Logger.LogWarning(ex, "[{Exchange}] Connection lost, reconnecting in {Delay}s", Name, delay.TotalSeconds);
                    else
                        Logger.LogWarning("[{Exchange}] Connect failed: {Msg}, retrying in {Delay}s", Name, ex.Message, delay.TotalSeconds);
                }

                try { await Task.Delay(delay, ct); }
                catch (OperationCanceledException) { return; }

                delay = TimeSpan.FromTicks(Math.Min(
                    (delay * 2).Ticks,
                    _config.MaxReconnectDelay.Ticks));
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task ReceiveAndWriteAsync(ClientWebSocket ws, ChannelWriter<Tick> writer, CancellationToken ct)
    {
        var buffer = new byte[8192];
        var sb = new StringBuilder();

        while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            sb.Clear();
            WebSocketReceiveResult result;
            bool closed = false;

            // ── Receive full message ──────────────────────────────────────────
            try
            {
                do
                {
                    result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        Logger.LogInformation("[{Exchange}] Server closed connection", Name);
                        closed = true;
                        break;
                    }

                    sb.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                }
                while (!result.EndOfMessage);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (WebSocketException)
            {
                return; // outer loop will reconnect
            }

            if (closed)
                return;

            var raw = sb.ToString();

            // ── Parse ─────────────────────────────────────────────────────────
            Tick? tick = null;
            try
            {
                tick = ParseMessage(raw);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[{Exchange}] Parse error: {Raw}", Name, raw);
            }

            if (tick is not null)
                await writer.WriteAsync(tick, ct);
        }
    }

    /// <summary>Called once after successful connect. Override to send subscription frames.</summary>
    protected virtual Task OnConnectedAsync(ClientWebSocket ws, CancellationToken ct) => Task.CompletedTask;

    /// <summary>Parse a raw WebSocket text message into a Tick. Return null to skip.</summary>
    protected abstract Tick? ParseMessage(string message);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
