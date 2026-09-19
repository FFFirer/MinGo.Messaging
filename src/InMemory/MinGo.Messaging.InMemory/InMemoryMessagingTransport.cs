using System.Threading.Channels;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Logging;

namespace MinGo.Messaging.InMemory;

/// <summary>
/// In-process implementation of <see cref="IMessagingTransport"/> backed by a shared
/// <see cref="InMemoryBus"/> singleton. Intended for unit tests, local development, and
/// single-process workloads — messages never leave the current CLR.
/// </summary>
internal sealed class InMemoryMessagingTransport : IMessagingTransport
{
    private readonly InMemoryBus _bus;
    private readonly ILogger<InMemoryMessagingTransport> _logger;
    private readonly List<Task> _consumerTasks = new();
    private readonly object _gate = new();
    private CancellationTokenSource? _cts;

    public InMemoryMessagingTransport(InMemoryBus bus, ILogger<InMemoryMessagingTransport> logger)
    {
        _bus = bus;
        _logger = logger;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        // Idempotent: repeated ConnectAsync calls keep the first CTS so in-flight consumers
        // are not orphaned by an accidental double-connect.
        Interlocked.CompareExchange(ref _cts, new CancellationTokenSource(), null);
        _logger.LogInformation("In-Memory transport connected.");
        return Task.CompletedTask;
    }

    public Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken = default)
    {
        EnsureConnected();
        return _bus.PublishAsync(topic, data, headers, cancellationToken).AsTask();
    }

    public Task SubscribeAsync(
        string topic,
        string subscriptionId,
        string group,
        DeliveryMode deliveryMode,
        Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken = default)
    {
        EnsureConnected();

        var cts = _cts!;
        var instanceId = Guid.NewGuid().ToString("N");
        var reader = _bus.Subscribe(topic, group, deliveryMode, subscriptionId, instanceId);

        var task = Task.Run(
            () => ConsumeLoopAsync(topic, reader, handler, cts.Token),
            CancellationToken.None);

        lock (_gate)
        {
            _consumerTasks.Add(task);
        }

        _logger.LogDebug(
            "In-Memory subscription registered: topic={Topic}, group={Group}, mode={Mode}.",
            topic, group, deliveryMode);

        return Task.CompletedTask;
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        var cts = Interlocked.Exchange(ref _cts, null);
        if (cts is null)
        {
            return;
        }

        await cts.CancelAsync().ConfigureAwait(false);

        Task[] snapshot;
        lock (_gate)
        {
            snapshot = [.. _consumerTasks];
            _consumerTasks.Clear();
        }

        try
        {
            await Task.WhenAll(snapshot).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown when the outer token trips first.
        }

        cts.Dispose();
        _logger.LogInformation("In-Memory transport disconnected.");
    }

    public ValueTask DisposeAsync()
    {
        _cts?.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task ConsumeLoopAsync(
        string topic,
        ChannelReader<InMemoryBus.Envelope> reader,
        Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var envelope in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    var headers = envelope.Headers.Count == 0
                        ? new Dictionary<string, object>()
                        : new Dictionary<string, object>(envelope.Headers);

                    var result = await handler(envelope.Payload, headers, cancellationToken).ConfigureAwait(false);

                    if (result != ConsumeResult.Ack)
                    {
                        // In-Memory has no persistent queue to redeliver from, so Nack/Retry
                        // are surfaced as a debug log and the message is dropped. Tests that
                        // need redelivery semantics should use a durable transport.
                        _logger.LogDebug(
                            "Handler for topic {Topic} returned {Result}; In-Memory transport discards the message.",
                            topic, result);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Unhandled exception in In-Memory consumer for topic {Topic}.",
                        topic);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown path.
        }
    }

    private void EnsureConnected()
    {
        if (_cts is null)
        {
            throw new InvalidOperationException(
                "In-Memory transport is not connected. Call ConnectAsync first.");
        }
    }
}
