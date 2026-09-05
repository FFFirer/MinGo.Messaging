using MinGo.Messaging.Transport;

namespace MinGo.Messaging.Pipeline;

/// <summary>
/// A delegate that represents the next step in the consumer pipeline.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
/// <param name="context">The consume context.</param>
/// <param name="cancellationToken">A token to cancel the operation.</param>
/// <returns>The result of the consume operation.</returns>
public delegate Task<ConsumeResult> ConsumerPipelineDelegate<TMessage>(
    ConsumeContext<TMessage> context,
    CancellationToken cancellationToken) where TMessage : IMessage;

/// <summary>
/// Middleware that participates in the consumer pipeline.
/// Middleware can inspect/modify the context, perform cross-cutting concerns,
/// and decide whether to call the next middleware in the chain.
/// </summary>
public interface IConsumerPipelineMiddleware
{
    /// <summary>
    /// Invokes the middleware as part of the consumer pipeline.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="context">The consume context.</param>
    /// <param name="next">The next delegate in the pipeline.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task InvokeAsync<TMessage>(
        ConsumeContext<TMessage> context,
        ConsumerPipelineDelegate<TMessage> next,
        CancellationToken cancellationToken) where TMessage : IMessage;
}
