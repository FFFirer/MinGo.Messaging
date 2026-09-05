using MinGo.Messaging.Internal;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
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
    /// Automatically discovers Integration SDKs and configures named endpoints.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration root.</param>
    /// <returns>A builder for further messaging configuration.</returns>
    public static IMessagingBuilder AddMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var builder = new MessagingBuilder(services, configuration);

        // Register default serializer (can be overridden via ConfigureSerializer)
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();

        // Run auto-configuration: discovers integrations, registers subscriptions, publishers
        var configurer = new MessagingAutoConfigurer(services, configuration, builder);
        configurer.Configure();

        return builder;
    }
}
