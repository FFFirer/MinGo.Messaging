using System.Reflection;
using MinGo.Messaging.Pipeline;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Transport;
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
    /// Auto-discovers and registers all typed message bus publishers.
    /// Scans the whole application dependency graph for interfaces decorated with
    /// <see cref="MessageBusAttribute"/> and resolves their transport from configuration.
    /// </summary>
    IMessagingBuilder AddPublishers();

    /// <summary>
    /// Auto-discovers and registers typed message bus publishers, restricting the scan to the
    /// assemblies selected from the application dependency graph by <paramref name="filter"/>.
    /// </summary>
    /// <param name="filter">
    /// A predicate evaluated against each candidate <see cref="AssemblyName"/> before it is loaded.
    /// See <see cref="AssemblyFilters"/> for ready-made filters.
    /// </param>
    IMessagingBuilder AddPublishers(Func<AssemblyName, bool> filter);

    /// <summary>
    /// Discovers and registers Integration SDKs, restricting the scan to the assemblies selected
    /// from the application dependency graph by <paramref name="filter"/>. Calling this claims
    /// integration discovery, so the implicit default (whole-graph) scan will not also run; call it
    /// before <see cref="AddPublishers()"/>/<see cref="AddConsumer(Assembly)"/> to take effect.
    /// </summary>
    /// <param name="filter">
    /// A predicate evaluated against each candidate <see cref="AssemblyName"/> before it is loaded.
    /// </param>
    IMessagingBuilder AddIntegrations(Func<AssemblyName, bool> filter);

    /// <summary>
    /// Explicitly registers a single integration's transport and its keyed
    /// <see cref="IMessagePublisher"/> — no assembly scanning involved. Calling this claims
    /// integration discovery, so the implicit default (whole-graph) scan will not also run.
    /// </summary>
    /// <remarks>
    /// This is the primitive behind the per-SDK convenience wrappers (e.g. <c>UseRabbitMQ</c>).
    /// The caller is responsible for registering any options/services the transport needs; the SDK
    /// wrappers do this for you. Registering the same <paramref name="name"/> twice is a no-op.
    /// </remarks>
    /// <typeparam name="TTransport">The <see cref="IMessagingTransport"/> implementation.</typeparam>
    /// <param name="name">
    /// The integration name, used as the keyed-service key and referenced by
    /// <c>Messaging:Publishers:{Bus}:Integration</c>.
    /// </param>
    IMessagingBuilder AddIntegration<TTransport>(string name)
        where TTransport : class, IMessagingTransport;

    /// <summary>
    /// Scans the specified assembly for consumer declarations and registers them.
    /// </summary>
    /// <param name="assembly">The assembly to scan for <see cref="MessageConsumerAttribute"/>.</param>
    IMessagingBuilder AddConsumer(Assembly assembly);

    /// <summary>
    /// Scans for consumer declarations in the assemblies selected from the application dependency
    /// graph by <paramref name="filter"/>, and registers them. May be called multiple times; the
    /// selected assemblies are combined (union) and de-duplicated.
    /// </summary>
    /// <param name="filter">
    /// A predicate evaluated against each candidate <see cref="AssemblyName"/> before it is loaded.
    /// </param>
    IMessagingBuilder AddConsumer(Func<AssemblyName, bool> filter);

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
