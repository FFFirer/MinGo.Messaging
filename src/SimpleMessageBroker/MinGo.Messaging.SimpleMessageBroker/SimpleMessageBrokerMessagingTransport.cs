using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;

namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// SimpleMessageBroker implementation of <see cref="IMessagingTransport"/>.
/// Uses JSON over HTTP to communicate with the SimpleMessageBroker server.
/// Since SimpleMessageBroker uses a pull-based consumption model,
/// this transport adapts it to the push-based <see cref="IMessagingTransport"/> contract
/// via background polling loops.
/// </summary>
internal sealed class SimpleMessageBrokerMessagingTransport : IMessagingTransport
{
    private readonly SimpleMessageBrokerIntegrationOptions _options;
    private readonly IHttpClientFactory _httpClientFactory;

    private HttpClient? _httpClient;
    private CancellationTokenSource? _pollingCts;
    private readonly List<Task> _pollingTasks = new();
    private readonly object _lock = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    public SimpleMessageBrokerMessagingTransport(
        SimpleMessageBrokerIntegrationOptions options,
        IHttpClientFactory httpClientFactory)
    {
        _options = options;
        _httpClientFactory = httpClientFactory;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _httpClient = _httpClientFactory.CreateClient("SimpleMessageBroker");
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

        // Convert headers to string dictionary for the SMB API
        Dictionary<string, string>? stringHeaders = null;
        if (headers.Count > 0)
        {
            stringHeaders = new Dictionary<string, string>(headers.Count);
            foreach (var kvp in headers)
            {
                stringHeaders[kvp.Key] = kvp.Value?.ToString() ?? string.Empty;
            }
        }

        var request = new
        {
            topic,
            payload = Convert.ToBase64String(data.ToArray()),
            contentType = "application/json",
            headers = stringHeaders
        };

        var response = await _httpClient!.PostAsJsonAsync(
            "api/v1/producer/messages", request, JsonOptions, cancellationToken);
        response.EnsureSuccessStatusCode();
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
                // Wait for all polling loops to finish gracefully
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
    /// Background polling loop that pulls messages from the SimpleMessageBroker server,
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
                var messages = await PullMessagesAsync(topic, consumerGroup, consumerId, cancellationToken);

                foreach (var message in messages)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    var payload = Convert.FromBase64String(message.PayloadBase64);

                    // Convert string headers to object headers
                    var headers = new Dictionary<string, object>();
                    if (message.Headers is not null)
                    {
                        foreach (var kvp in message.Headers)
                        {
                            headers[kvp.Key] = kvp.Value;
                        }
                    }

                    var result = await handler(payload, headers, cancellationToken);

                    if (result == ConsumeResult.Ack)
                    {
                        await AcknowledgeAsync(message.Id, consumerGroup, consumerId, cancellationToken);
                    }
                    // Nack and Retry are implicitly handled: un-acked messages remain
                    // available for re-consumption in the SimpleMessageBroker server.
                }

                if (messages.Count == 0)
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

    private async Task<List<ConsumedMessageDto>> PullMessagesAsync(
        string topic,
        string consumerGroup,
        string? consumerId,
        CancellationToken cancellationToken)
    {
        var request = new
        {
            topic,
            consumerGroup,
            consumerId,
            batchSize = _options.BatchSize,
            timeoutSeconds = _options.ConsumeTimeoutSeconds
        };

        var response = await _httpClient!.PostAsJsonAsync(
            "api/v1/consumer/pull", request, JsonOptions, cancellationToken);
        response.EnsureSuccessStatusCode();

        var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<ConsumeResultDto>>(
            JsonOptions, cancellationToken);

        return apiResponse?.Data?.Messages ?? [];
    }

    private async Task AcknowledgeAsync(
        string messageId,
        string consumerGroup,
        string? consumerId,
        CancellationToken cancellationToken)
    {
        var url = $"api/v1/consumer/ack/{Uri.EscapeDataString(messageId)}?consumerGroup={Uri.EscapeDataString(consumerGroup)}";
        if (consumerId is not null)
        {
            url += $"&consumerId={Uri.EscapeDataString(consumerId)}";
        }

        var response = await _httpClient!.PostAsync(url, content: null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    private void EnsureConnected()
    {
        if (_httpClient is null || _pollingCts is null)
        {
            throw new InvalidOperationException("Transport is not connected. Call ConnectAsync first.");
        }
    }

    // ---- Internal DTOs for SimpleMessageBroker REST API ----

    private sealed class ApiResponse<T>
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public T? Data { get; set; }
        public string? ErrorCode { get; set; }
    }

    private sealed class ConsumeResultDto
    {
        public List<ConsumedMessageDto> Messages { get; set; } = [];
        public int Count { get; set; }
        public bool HasMore { get; set; }
    }

    private sealed class ConsumedMessageDto
    {
        public string Id { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public string? Key { get; set; }
        public int Partition { get; set; }
        public string PayloadBase64 { get; set; } = string.Empty;
        public string ContentType { get; set; } = string.Empty;
        public Dictionary<string, string>? Headers { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
