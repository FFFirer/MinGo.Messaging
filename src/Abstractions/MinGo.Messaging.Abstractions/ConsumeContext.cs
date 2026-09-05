using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging;

/// <summary>
/// Provides context for a consumed message, including metadata about the delivery.
/// </summary>
/// <typeparam name="TMessage">The type of the consumed message.</typeparam>
public sealed class ConsumeContext<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Gets the consumed message.
    /// </summary>
    public required TMessage Message { get; init; }

    /// <summary>
    /// Gets the unique identifier for this message delivery.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Gets the correlation identifier for distributed tracing.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the subscription identifier that triggered this delivery.
    /// </summary>
    public required string SubscriptionId { get; init; }

    /// <summary>
    /// Gets the consumer group identifier.
    /// </summary>
    public required string ConsumerGroupId { get; init; }

    /// <summary>
    /// Gets the consumer instance identifier, if applicable.
    /// </summary>
    public string? ConsumerInstanceId { get; init; }

    /// <summary>
    /// Gets the delivery mode for this consumption.
    /// </summary>
    public required DeliveryMode DeliveryMode { get; init; }

    /// <summary>
    /// Gets the timestamp when the message was produced.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the number of delivery attempts (1 = first attempt).
    /// </summary>
    public int AttemptCount { get; init; } = 1;

    /// <summary>
    /// Gets the message headers/metadata.
    /// </summary>
    public IDictionary<string, object> Headers { get; init; } = new Dictionary<string, object>();

    /// <summary>
    /// Gets the cancellation token for this consumption.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }
}
