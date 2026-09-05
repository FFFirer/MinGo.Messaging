using MinGo.Messaging.Integration;

namespace MinGo.Messaging.RabbitMQ;

/// <summary>
/// Configuration options for the RabbitMQ integration.
/// </summary>
public sealed class RabbitMQIntegrationOptions : MessagingIntegrationOptions
{
    /// <summary>
    /// Gets or sets the AMQP connection string (e.g. "amqp://guest:guest@localhost:5672").
    /// </summary>
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672";

    /// <summary>
    /// Gets or sets the default exchange name. Defaults to "amq.topic".
    /// </summary>
    public string Exchange { get; set; } = "amq.topic";

    /// <summary>
    /// Gets or sets the exchange type. Defaults to "topic".
    /// </summary>
    public string ExchangeType { get; set; } = "topic";

    /// <summary>
    /// Gets or sets whether exchanges and queues should be durable. Defaults to true.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Gets or sets the prefetch count for consumers. Defaults to 10.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;
}
