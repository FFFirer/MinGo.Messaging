namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Defines the type of delivery target for a subscription.
/// </summary>
public enum DeliveryTargetType
{
    /// <summary>
    /// Deliver to a consumer group (competing consumers by default).
    /// </summary>
    ConsumerGroup,

    /// <summary>
    /// Deliver to a specific consumer instance.
    /// </summary>
    Instance,

    /// <summary>
    /// Deliver to instances matching a selector.
    /// </summary>
    InstanceSelector
}
