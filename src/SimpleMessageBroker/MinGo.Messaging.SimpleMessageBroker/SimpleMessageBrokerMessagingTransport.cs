using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using SimpleMessageBroker.Client;

namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// SimpleMessageBroker implementation of <see cref="IMessagingTransport"/>.
/// Uses the SimpleMessageBroker.Client SDK to communicate with the server.
/// Since SimpleMessageBroker uses a pull-based consumption model,
/// this transport adapts it to the push-based <see cref="IMessagingTransport"/> contract
/// via background polling loops.
/// </summary>
internal sealed class SimpleMessageBrokerMessagingTransport : IMessagingTransport
{
    private readonly SimpleMessageBrokerIntegrationOptions _options;
    private readonly IMessageQueueClient _client;

    private CancellationTokenSource? _pollingCts;
    private readonly List<Task> _pollingTasks = new();
    private readonly object _lock = new();

    public SimpleMessageBrokerMessagingTransport(
        SimpleMessageBrokerIntegrationOptions options,
        IMessageQueueClient client)
    {
        _options = options;
        _client = client;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _pollingCts = new CancellationTokenSource();
        return Task.CompletedTask;
    }

    public async Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken = default)
    {
        EnsureConnected();

        // Convert object headers to string dictionary for the SMB SDK
        Dictionary<string, string>? stringHeaders = null;
        if (headers.Count > 0)
        {
            stringHeaders = new Dictionary<string, string>(headers.Count);
            foreach (var kvp in headers)
            {
                stringHeaders[kvp.Key] = kvp.Value?.ToString() ?? string.Empty;
            }
        }

        await _client.ProduceAsync(
            topic,
            data.ToArray(),
            contentType: "application/json",
            headers: stringHeaders,
            cancellationToken: cancellationToken);
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

        // Build the consumer ID based on delivery mode:
        // - Competing: shared consumer ID within the group (all instances compete)
        // - Broadcast: unique consumer ID per instance (each instance gets all messages)
        var consumerId = deliveryMode == DeliveryMode.Broadcast
            ? $"{group}-{subscriptionId}-{Guid.NewGuid():N}"
            : null;

        var task = Task.Run(
            () => PollingLoopAsync(topic, group, consumerId, handler, _pollingCts!.Token),
            _pollingCts!.Token);

        lock (_lock)
        {
            _pollingTasks.Add(task);
        }

        return Task.CompletedTask;
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (_pollingCts is not null)
        {
            await _pollingCts.CancelAsync();

            try
            {
                Task[] tasks;
                lock (_lock)
                {
                    tasks = [.. _pollingTasks];
                }

                await Task.WhenAll(tasks).WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _pollingCts?.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Background polling loop that pulls messages via the SimpleMessageBroker SDK,
    /// dispatches them to the handler, and acknowledges successful processing.
    /// </summary>
    private async Task PollingLoopAsync(
        string topic,
        string consumerGroup,
        string? consumerId,
        Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await _client.ConsumeAsync(
                    topic,
                    consumerGroup,
                    consumerId,
                    batchSize: _options.BatchSize,
                    timeoutSeconds: _options.ConsumeTimeoutSeconds,
                    cancellationToken: cancellationToken);

                foreach (var message in result.Messages)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    // Convert string headers to object headers
                    var headers = new Dictionary<string, object>();
                    if (message.Headers is not null)
                    {
                        foreach (var kvp in message.Headers)
                        {
                            headers[kvp.Key] = kvp.Value;
                        }
                    }

                    var consumeResult = await handler(message.Payload, headers, cancellationToken);

                    if (consumeResult == ConsumeResult.Ack)
                    {
                        await _client.AcknowledgeAsync(
                            message.Id, consumerGroup, consumerId, cancellationToken);
                    }
                    // Nack and Retry are implicitly handled: un-acked messages remain
                    // available for re-consumption on the SimpleMessageBroker server.
                }

                if (result.Count == 0)
                {
                    // No messages available — wait before next poll
                    await Task.Delay(_options.PollingInterval, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception)
            {
                // Transient error — wait before retrying to avoid tight error loops
                try
                {
                    await Task.Delay(_options.PollingInterval, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private void EnsureConnected()
    {
        if (_pollingCts is null)
        {
            throw new InvalidOperationException("Transport is not connected. Call ConnectAsync first.");
        }
    }
}
