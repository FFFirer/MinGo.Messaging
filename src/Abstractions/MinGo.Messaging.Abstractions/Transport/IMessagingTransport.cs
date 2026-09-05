using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging.Transport;

/// <summary>
/// The result of a consume operation, indicating how the transport should handle the message.
/// </summary>
public enum ConsumeResult
{
    /// <summary>
    /// Message was successfully processed. The transport should acknowledge it.
    /// </summary>
    Ack,

    /// <summary>
    /// Message processing failed permanently. The transport should negatively acknowledge it.
    /// </summary>
    Nack,

    /// <summary>
    /// Message processing failed temporarily. The transport should retry or redeliver it.
    /// </summary>
    Retry
}

/// <summary>
/// Service Provider Interface (SPI) between the Base SDK and Integration SDKs.
/// This is the ONLY technical boundary between the two layers.
/// Each Integration SDK implements this interface for its specific middleware.
/// </summary>
public interface IMessagingTransport : IAsyncDisposable
{
    /// <summary>
    /// Establishes the connection to the messaging middleware.
    /// </summary>
    Task ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a serialized message to a topic.
    /// </summary>
    /// <param name="topic">The topic/routing key to publish to.</param>
    /// <param name="data">The serialized message bytes.</param>
    /// <param name="headers">Message headers/metadata.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to a topic with the specified delivery semantics.
    /// </summary>
    /// <param name="topic">The topic/routing key to subscribe to.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="deliveryMode">The delivery mode (Competing or Broadcast).</param>
    /// <param name="handler">The handler to invoke for each received message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task SubscribeAsync(
        string topic,
        string subscriptionId,
        string group,
        DeliveryMode deliveryMode,
        Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Disconnects from the messaging middleware gracefully.
    /// </summary>
    Task DisconnectAsync(CancellationToken cancellationToken = default);
}
