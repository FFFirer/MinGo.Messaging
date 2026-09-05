namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Defines how messages are delivered to consumers within a subscription.
/// </summary>
public enum DeliveryMode
{
    /// <summary>
    /// Competing consumers: within a consumer group, only one instance processes each message.
    /// This is the default mode for load-balanced consumption.
    /// </summary>
    Competing,

    /// <summary>
    /// Broadcast: all specified instances receive every message.
    /// Used for scenarios like configuration refresh or cache invalidation.
    /// </summary>
    Broadcast
}
