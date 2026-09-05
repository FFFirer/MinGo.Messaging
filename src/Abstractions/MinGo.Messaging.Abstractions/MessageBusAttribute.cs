namespace MinGo.Messaging;

/// <summary>
/// Marks an interface as a typed message bus publisher.
/// The interface must extend <see cref="IMessagePublisher"/>.
/// The <see cref="Name"/> determines which integration/transport is used for delivery,
/// as declared in configuration under <c>Messaging:Publishers:{Name}</c>.
/// </summary>
/// <example>
/// <code>
/// [MessageBus("OrderBus")]
/// public interface IOrderBusPublisher : IMessagePublisher { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Interface, AllowMultiple = false, Inherited = false)]
public sealed class MessageBusAttribute : Attribute
{
    /// <summary>
    /// Gets the logical bus name (e.g. "OrderBus").
    /// This name maps to a publisher entry in configuration that specifies which integration to use.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageBusAttribute"/> class.
    /// </summary>
    /// <param name="name">The logical bus name.</param>
    public MessageBusAttribute(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }
}
