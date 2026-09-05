using System.Text;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace MinGo.Messaging.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IMessagingTransport"/>.
/// Uses RabbitMQ.Client to provide topic-based routing with competing consumer and broadcast delivery modes.
/// Supports automatic connection recovery and manual reconnection with topology restoration.
/// </summary>
internal sealed class RabbitMQMessagingTransport : IMessagingTransport
{
    private readonly RabbitMQIntegrationOptions _options;
    private readonly ILogger<RabbitMQMessagingTransport> _logger;
    private ConnectionFactory? _factory;
    private IConnection? _connection;
    private IModel? _channel;

    // Track subscription descriptors for topology restoration after manual recovery
    private readonly List<SubscriptionDescriptor> _subscriptions = new();
    private readonly object _stateLock = new();
    private volatile bool _isDisposed;
    private volatile bool _isShuttingDown;

    /// <summary>
    /// Describes a subscription that needs to be restored after reconnection.
    /// </summary>
    private sealed class SubscriptionDescriptor
    {
        public required string Topic { get; init; }
        public required string QueueName { get; init; }
        public required bool Exclusive { get; init; }
        public required bool Durable { get; init; }
        public required Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> Handler { get; init; }
    }

    public RabbitMQMessagingTransport(
        RabbitMQIntegrationOptions options,
        ILogger<RabbitMQMessagingTransport> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString),
            AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
            NetworkRecoveryInterval = _options.RecoveryInterval
        };

        var maxRetries = _options.MaxConnectionRetries;
        var retryDelay = _options.ConnectionRetryDelay;

        for (var attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                _connection = _factory.CreateConnection();
                _channel = _connection.CreateModel();

                // Declare the exchange
                _channel.ExchangeDeclare(
                    exchange: _options.Exchange,
                    type: _options.ExchangeType,
                    durable: _options.Durable,
                    autoDelete: false);

                // Wire up connection lifecycle events
                _connection.ConnectionShutdown += OnConnectionShutdown;
                _connection.ConnectionBlocked += OnConnectionBlocked;
                _connection.ConnectionUnblocked += OnConnectionUnblocked;

                _logger.LogInformation(
                    "RabbitMQ transport connected to {Exchange} (auto-recovery={AutoRecovery}).",
                    _options.Exchange,
                    _options.AutomaticRecoveryEnabled);

                // Restore subscriptions if this is a reconnect
                await RestoreSubscriptionsAsync(cancellationToken);

                return;
            }
            catch (Exception ex) when (attempt < maxRetries || maxRetries == -1)
            {
                _logger.LogWarning(
                    ex,
                    "RabbitMQ connection attempt {Attempt} failed. Retrying in {Delay}...",
                    attempt + 1,
                    retryDelay);

                await Task.Delay(retryDelay, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ connection failed after {Attempts} attempts.", attempt + 1);
                throw;
            }
        }
    }

    public async Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken = default)
    {
        ThrowIfNotConnected();

        // Retry publish briefly to survive the auto-recovery window
        var maxAttempts = _options.AutomaticRecoveryEnabled ? 3 : 1;

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                var channel = _channel;
                if (channel is null || !channel.IsOpen)
                {
                    if (attempt < maxAttempts - 1)
                    {
                        _logger.LogDebug("Channel not open during publish, waiting for recovery...");
                        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                        continue;
                    }

                    throw new InvalidOperationException(
                        "RabbitMQ channel is not open and recovery has not restored it.");
                }

                var properties = channel.CreateBasicProperties();
                properties.Headers = new Dictionary<string, object>();

                foreach (var header in headers)
                {
                    properties.Headers[header.Key] = header.Value switch
                    {
                        string s => Encoding.UTF8.GetBytes(s),
                        byte[] b => b,
                        _ => Encoding.UTF8.GetBytes(header.Value.ToString() ?? string.Empty)
                    };
                }

                properties.ContentType = "application/json";
                properties.DeliveryMode = 2; // Persistent

                channel.BasicPublish(
                    exchange: _options.Exchange,
                    routingKey: topic,
                    basicProperties: properties,
                    body: data.ToArray());

                return;
            }
            catch (Exception ex) when (
                attempt < maxAttempts - 1 &&
                ex is AlreadyClosedException or IOException)
            {
                _logger.LogWarning(
                    ex,
                    "Publish failed (attempt {Attempt}/{Max}), waiting for recovery...",
                    attempt + 1,
                    maxAttempts);

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
        }
    }

    public Task SubscribeAsync(
        string topic,
        string subscriptionId,
        string group,
        DeliveryMode deliveryMode,
        Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ThrowIfNotConnected();

        // Determine queue name based on delivery mode
        string queueName;
        bool exclusive;

        if (deliveryMode == DeliveryMode.Broadcast)
        {
            // Broadcast: each instance gets its own exclusive queue
            queueName = $"{group}-{subscriptionId}-{Guid.NewGuid():N}";
            exclusive = true;
        }
        else
        {
            // Competing: all instances in the same group share a queue
            queueName = $"{group}-{topic}";
            exclusive = false;
        }

        var descriptor = new SubscriptionDescriptor
        {
            Topic = topic,
            QueueName = queueName,
            Exclusive = exclusive,
            Durable = !exclusive && _options.Durable,
            Handler = handler
        };

        lock (_stateLock)
        {
            _subscriptions.Add(descriptor);
        }

        StartConsumer(descriptor);

        return Task.CompletedTask;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        _isShuttingDown = true;

        // Cancel all consumers
        lock (_stateLock)
        {
            if (_channel is { IsOpen: true })
            {
                foreach (var sub in _subscriptions)
                {
                    try
                    {
                        // Consumer tags are managed internally by the channel;
                        // closing the channel will cancel all consumers.
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Error cancelling consumer for queue {Queue}.", sub.QueueName);
                    }
                }

                try
                {
                    _channel.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Error closing channel during disconnect.");
                }
            }
        }

        try
        {
            _connection?.Close();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error closing connection during disconnect.");
        }

        _logger.LogInformation("RabbitMQ transport disconnected.");

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_isDisposed) return ValueTask.CompletedTask;
        _isDisposed = true;

        _channel?.Dispose();
        _connection?.Dispose();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Starts a consumer on the current channel for the given subscription descriptor.
    /// </summary>
    private void StartConsumer(SubscriptionDescriptor descriptor)
    {
        var channel = _channel ?? throw new InvalidOperationException("Channel is not initialized.");

        // Declare queue
        channel.QueueDeclare(
            queue: descriptor.QueueName,
            durable: descriptor.Durable,
            exclusive: descriptor.Exclusive,
            autoDelete: descriptor.Exclusive,
            arguments: null);

        // Bind queue to exchange with routing key
        channel.QueueBind(
            queue: descriptor.QueueName,
            exchange: _options.Exchange,
            routingKey: descriptor.Topic);

        // Set prefetch
        channel.BasicQos(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false);

        // Create consumer
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            var headers = new Dictionary<string, object>();
            if (ea.BasicProperties?.Headers != null)
            {
                foreach (var header in ea.BasicProperties.Headers)
                {
                    headers[header.Key] = header.Value switch
                    {
                        byte[] bytes => Encoding.UTF8.GetString(bytes),
                        _ => header.Value
                    };
                }
            }

            var result = await descriptor.Handler(ea.Body, headers, CancellationToken.None);

            switch (result)
            {
                case ConsumeResult.Ack:
                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                    break;
                case ConsumeResult.Nack:
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                    break;
                case ConsumeResult.Retry:
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
                    break;
            }
        };

        channel.BasicConsume(
            queue: descriptor.QueueName,
            autoAck: false,
            consumer: consumer);

        _logger.LogDebug(
            "Consumer started for queue {Queue} (exclusive={Exclusive}).",
            descriptor.QueueName,
            descriptor.Exclusive);
    }

    /// <summary>
    /// Restores all tracked subscriptions by re-declaring queues and restarting consumers.
    /// Called after a reconnection to re-establish the topology.
    /// </summary>
    private async Task RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        List<SubscriptionDescriptor> snapshot;
        lock (_stateLock)
        {
            if (_subscriptions.Count == 0) return;
            snapshot = [.. _subscriptions];
        }

        _logger.LogInformation("Restoring {Count} subscription(s)...", snapshot.Count);

        foreach (var descriptor in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                StartConsumer(descriptor);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to restore subscription for queue {Queue}.", descriptor.QueueName);
            }
        }

        await Task.CompletedTask;
    }

    /// <summary>
    /// Handles connection shutdown events. Triggers manual recovery if automatic recovery is disabled.
    /// </summary>
    private void OnConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        if (_isShuttingDown || _isDisposed)
        {
            _logger.LogInformation("RabbitMQ connection closed (initiator={Initiator}).", e.Initiator);
            return;
        }

        _logger.LogWarning(
            "RabbitMQ connection lost (initiator={Initiator}, reason={Reason}).",
            e.Initiator,
            e.ReplyText);

        if (!_options.AutomaticRecoveryEnabled)
        {
            // Manual recovery: attempt to reconnect in the background
            _ = Task.Run(() => ManualRecoveryLoopAsync());
        }
    }

    /// <summary>
    /// Handles connection blocked events (e.g. memory or disk alarm on the broker).
    /// </summary>
    private void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
    {
        _logger.LogWarning("RabbitMQ connection blocked: {Reason}", e.Reason);
    }

    /// <summary>
    /// Handles connection unblocked events.
    /// </summary>
    private void OnConnectionUnblocked(object? sender, EventArgs e)
    {
        _logger.LogInformation("RabbitMQ connection unblocked.");
    }

    /// <summary>
    /// Manual recovery loop that attempts to reconnect and restore topology
    /// when automatic recovery is disabled.
    /// </summary>
    private async Task ManualRecoveryLoopAsync()
    {
        if (_isDisposed || _isShuttingDown) return;

        var attempt = 0;

        while (!_isDisposed && !_isShuttingDown)
        {
            try
            {
                attempt++;
                _logger.LogInformation("Manual recovery attempt {Attempt}...", attempt);

                if (_factory is null) return;

                // Dispose old connection/channel
                try { _channel?.Dispose(); } catch { /* ignore */ }
                try { _connection?.Dispose(); } catch { /* ignore */ }

                _connection = _factory.CreateConnection();
                _channel = _connection.CreateModel();

                // Re-declare exchange
                _channel.ExchangeDeclare(
                    exchange: _options.Exchange,
                    type: _options.ExchangeType,
                    durable: _options.Durable,
                    autoDelete: false);

                // Wire up events again
                _connection.ConnectionShutdown += OnConnectionShutdown;
                _connection.ConnectionBlocked += OnConnectionBlocked;
                _connection.ConnectionUnblocked += OnConnectionUnblocked;

                // Restore subscriptions
                await RestoreSubscriptionsAsync(CancellationToken.None);

                _logger.LogInformation("Manual recovery succeeded on attempt {Attempt}.", attempt);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Manual recovery attempt {Attempt} failed. Retrying...", attempt);
                await Task.Delay(_options.RecoveryInterval);
            }
        }
    }

    private void ThrowIfNotConnected()
    {
        if (_channel is null || _connection is null)
        {
            throw new InvalidOperationException("Transport is not connected. Call ConnectAsync first.");
        }
    }
}
