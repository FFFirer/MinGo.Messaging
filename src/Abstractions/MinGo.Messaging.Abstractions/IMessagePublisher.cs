namespace MinGo.Messaging;

/// <summary>
/// Publishes messages to the default messaging endpoint.
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

/// <summary>
/// Publishes messages to a named messaging endpoint.
/// The name determines which integration/middleware is used for delivery.
/// </summary>
public interface INamedMessagePublisher
{
    /// <summary>
    /// Publishes an event message to all subscribers via the named endpoint.
    /// </summary>
    Task PublishAsync(string name, IMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a command message to a single consumer via the named endpoint.
    /// </summary>
    Task SendAsync(string name, IMessage message, CancellationToken cancellationToken = default);
}
