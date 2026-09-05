using System.Reflection;
using MinGo.Messaging.Pipeline;
using MinGo.Messaging.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging;

/// <summary>
/// Fluent builder for configuring the messaging system.
/// </summary>
public interface IMessagingBuilder
{
    /// <summary>
    /// Gets the service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Gets the configuration root.
    /// </summary>
    IConfiguration Configuration { get; }

    /// <summary>
    /// Registers a named publisher for the specified message type.
    /// </summary>
    /// <param name="name">The publisher/endpoint name.</param>
    IMessagingBuilder AddPublisher(string name);

    /// <summary>
    /// Scans the specified assembly for consumer declarations and registers them.
    /// </summary>
    /// <param name="assembly">The assembly to scan for <see cref="MessageConsumerAttribute"/>.</param>
    IMessagingBuilder AddConsumer(Assembly assembly);

    /// <summary>
    /// Replaces the default message serializer with a custom implementation.
    /// </summary>
    /// <param name="serializer">The custom serializer to use.</param>
    IMessagingBuilder ConfigureSerializer(IMessageSerializer serializer);

    /// <summary>
    /// Adds a middleware to the consumer pipeline.
    /// </summary>
    /// <typeparam name="TMiddleware">The middleware type.</typeparam>
    IMessagingBuilder AddPipelineMiddleware<TMiddleware>() where TMiddleware : class, IConsumerPipelineMiddleware;
}
