using MinGo.Messaging.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MinGo.Messaging;

/// <summary>
/// Extension methods for registering messaging services.
/// </summary>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Adds the messaging system to the service collection.
    /// Integration SDKs, consumers and typed publishers are discovered as the returned builder's
    /// <c>AddIntegrations</c>/<c>AddConsumer</c>/<c>AddPublishers</c> methods are chained, optionally
    /// narrowed by <see cref="System.Reflection.AssemblyName"/> filters over the dependency graph.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration root.</param>
    /// <returns>A builder for further messaging configuration.</returns>
    public static IMessagingBuilder AddMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        // Register default serializer (can be overridden via ConfigureSerializer)
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();

        // The builder owns discovery; it registers the shared SubscriptionRegistry and performs
        // integration/consumer/publisher registration as the fluent chain is executed.
        return new MessagingBuilder(services, configuration);
    }
}
