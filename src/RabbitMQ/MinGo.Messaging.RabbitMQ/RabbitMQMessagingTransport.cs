using System.Text;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MinGo.Messaging.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IMessagingTransport"/>.
/// Uses RabbitMQ.Client to provide topic-based routing with competing consumer and broadcast delivery modes.
/// </summary>
internal sealed class RabbitMQMessagingTransport : IMessagingTransport
{
    private readonly RabbitMQIntegrationOptions _options;
    private ConnectionFactory? _factory;
    private IConnection? _connection;
    private IModel? _channel;
    private readonly List<string> _consumerTags = new();

    public RabbitMQMessagingTransport(RabbitMQIntegrationOptions options)
    {
        _options = options;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString)
        };

        _connection = _factory.CreateConnection();
        _channel = _connection.CreateModel();

        // Declare the exchange
        _channel.ExchangeDeclare(
            exchange: _options.Exchange,
            type: _options.ExchangeType,
            durable: _options.Durable,
            autoDelete: false);

        return Task.CompletedTask;
    }

    public Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken = default)
    {
        EnsureConnected();

        var properties = _channel!.CreateBasicProperties();
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

        _channel.BasicPublish(
            exchange: _options.Exchange,
            routingKey: topic,
            basicProperties: properties,
            body: data.ToArray());

        return Task.CompletedTask;
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

        // Declare queue
        _channel!.QueueDeclare(
            queue: queueName,
            durable: !exclusive && _options.Durable,
            exclusive: exclusive,
            autoDelete: exclusive,
            arguments: null);

        // Bind queue to exchange with routing key
        _channel.QueueBind(
            queue: queueName,
            exchange: _options.Exchange,
            routingKey: topic);

        // Set prefetch
        _channel.BasicQos(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false);

        // Create consumer
        var consumer = new AsyncEventingBasicConsumer(_channel);
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

            var result = await handler(ea.Body, headers, cancellationToken);

            switch (result)
            {
                case ConsumeResult.Ack:
                    _channel.BasicAck(ea.DeliveryTag, multiple: false);
                    break;
                case ConsumeResult.Nack:
                    _channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                    break;
                case ConsumeResult.Retry:
                    _channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
                    break;
            }
        };

        var consumerTag = _channel.BasicConsume(
            queue: queueName,
            autoAck: false,
            consumer: consumer);

        _consumerTags.Add(consumerTag);

        return Task.CompletedTask;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        // Cancel all consumers
        if (_channel is { IsOpen: true })
        {
            foreach (var tag in _consumerTags)
            {
                try
                {
                    _channel.BasicCancel(tag);
                }
                catch
                {
                    // Ignore errors during shutdown
                }
            }

            _channel.Close();
        }

        _connection?.Close();

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _channel?.Dispose();
        _connection?.Dispose();
        return ValueTask.CompletedTask;
    }

    private void EnsureConnected()
    {
        if (_channel is null || _connection is null)
        {
            throw new InvalidOperationException("Transport is not connected. Call ConnectAsync first.");
        }
    }
}
