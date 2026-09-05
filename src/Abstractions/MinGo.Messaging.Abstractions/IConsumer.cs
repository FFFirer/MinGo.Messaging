namespace MinGo.Messaging;

/// <summary>
/// Defines a consumer for a specific message type.
/// </summary>
/// <typeparam name="TMessage">The type of message to consume.</typeparam>
public interface IConsumer<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Handles the consumed message.
    /// </summary>
    /// <param name="context">The consume context containing the message and metadata.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task ConsumeAsync(ConsumeContext<TMessage> context, CancellationToken cancellationToken = default);
}
