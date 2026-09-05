using MinGo.Messaging.Integration;

namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// Configuration options for the SimpleMessageBroker integration.
/// </summary>
public sealed class SimpleMessageBrokerIntegrationOptions : MessagingIntegrationOptions
{
    /// <summary>
    /// Gets or sets the base address of the SimpleMessageBroker server
    /// (e.g. "http://localhost:5000").
    /// </summary>
    public string BaseAddress { get; set; } = "http://localhost:5000";

    /// <summary>
    /// Gets or sets the optional API key for authentication.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Gets or sets the polling interval for pull-based message consumption.
    /// Defaults to 1 second.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the maximum number of messages to pull per batch.
    /// Defaults to 10.
    /// </summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// Gets or sets the long-poll timeout for each consume request.
    /// The server will hold the request open up to this duration waiting for messages.
    /// Defaults to 5 seconds.
    /// </summary>
    public int ConsumeTimeoutSeconds { get; set; } = 5;

    /// <summary>
    /// Gets or sets the maximum number of HTTP connections per server.
    /// Defaults to 50.
    /// </summary>
    public int MaxConnectionsPerServer { get; set; } = 50;

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
