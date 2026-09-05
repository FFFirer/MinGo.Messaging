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

    /// <summary>
    /// Gets or sets whether automatic connection recovery is enabled.
    /// When true, the RabbitMQ client will automatically reconnect after a connection failure.
    /// Defaults to true.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the interval between automatic recovery attempts. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan RecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the maximum number of retries for the initial connection attempt.
    /// Defaults to 5. Set to -1 for infinite retries.
    /// </summary>
    public int MaxConnectionRetries { get; set; } = 5;

    /// <summary>
    /// Gets or sets the delay between initial connection retry attempts. Defaults to 2 seconds.
    /// </summary>
    public TimeSpan ConnectionRetryDelay { get; set; } = TimeSpan.FromSeconds(2);
}
