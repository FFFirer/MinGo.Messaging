using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging;

/// <summary>
/// Declares that a class consumes a specific message contract.
/// A single consumer class can declare multiple attributes to consume different contracts.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class MessageConsumerAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the message contract type this consumer handles.
    /// </summary>
    public Type Contract { get; set; } = typeof(object);

    /// <summary>
    /// Gets or sets the consumer group name. Consumers in the same group compete for messages.
    /// </summary>
    public string Group { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the delivery mode. Defaults to <see cref="DeliveryMode.Competing"/>.
    /// </summary>
    public DeliveryMode DeliveryMode { get; set; } = DeliveryMode.Competing;
}
