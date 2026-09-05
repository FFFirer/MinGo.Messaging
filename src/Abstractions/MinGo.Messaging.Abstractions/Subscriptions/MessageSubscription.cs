namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Represents a business-level subscription binding between a message contract and a consumer service.
/// A subscription answers the question: "Which business service needs this event?"
/// </summary>
public sealed class MessageSubscription
{
    /// <summary>
    /// Gets the unique identifier for this subscription.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the contract identifier (from <see cref="MessageContractAttribute.Id"/>).
    /// </summary>
    public required string ContractId { get; init; }

    /// <summary>
    /// Gets the contract version (from <see cref="MessageContractAttribute.Version"/>).
    /// </summary>
    public required string ContractVersion { get; init; }

    /// <summary>
    /// Gets the consumer service identifier that owns this subscription.
    /// </summary>
    public required string ConsumerServiceId { get; init; }

    /// <summary>
    /// Gets the delivery mode for this subscription. Defaults to <see cref="DeliveryMode.Competing"/>.
    /// </summary>
    public DeliveryMode DeliveryMode { get; init; } = DeliveryMode.Competing;

    /// <summary>
    /// Gets the delivery target for this subscription.
    /// </summary>
    public DeliveryTarget Target { get; init; } = new() { Type = DeliveryTargetType.ConsumerGroup };
}
