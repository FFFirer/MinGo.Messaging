namespace MinGo.Messaging;

/// <summary>
/// Publishes messages to a messaging endpoint.
/// Concrete sub-interfaces (e.g. IOrderBusPublisher) represent typed message buses
/// and are auto-discovered via <see cref="MessageBusAttribute"/>.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes an event message to all subscribers.
    /// </summary>
    Task PublishAsync(IMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a command message to a single consumer.
    /// </summary>
    Task SendAsync(IMessage message, CancellationToken cancellationToken = default);
}
