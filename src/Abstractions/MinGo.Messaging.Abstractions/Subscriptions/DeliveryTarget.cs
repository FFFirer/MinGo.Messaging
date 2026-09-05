namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Defines where messages should be delivered for a subscription.
/// Supports consumer groups, specific instances, or instance selectors.
/// </summary>
public sealed class DeliveryTarget
{
    /// <summary>
    /// Gets the type of delivery target.
    /// </summary>
    public required DeliveryTargetType Type { get; init; }

    /// <summary>
    /// Gets the consumer group identifier. Used when <see cref="Type"/> is <see cref="DeliveryTargetType.ConsumerGroup"/>.
    /// </summary>
    public string? ConsumerGroupId { get; init; }

    /// <summary>
    /// Gets the consumer instance identifier. Used when <see cref="Type"/> is <see cref="DeliveryTargetType.Instance"/>.
    /// </summary>
    public string? ConsumerInstanceId { get; init; }

    /// <summary>
    /// Gets the instance selector. Used when <see cref="Type"/> is <see cref="DeliveryTargetType.InstanceSelector"/>.
    /// </summary>
    public InstanceSelector? Selector { get; init; }
}
