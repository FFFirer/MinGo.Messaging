using System.Reflection;
using MinGo.Messaging.Integration;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Automatically configures the messaging system based on conventions:
/// discovers Integration SDKs, loads configuration, registers keyed services,
/// and builds consumer pipelines.
/// </summary>
internal sealed class MessagingAutoConfigurer
{
    private readonly IServiceCollection _services;
    private readonly IConfiguration _configuration;
    private readonly MessagingBuilder _builder;

    public MessagingAutoConfigurer(IServiceCollection services, IConfiguration configuration, MessagingBuilder builder)
    {
        _services = services;
        _configuration = configuration;
        _builder = builder;
    }

    public void Configure()
    {
        // 1. Discover Integration SDKs
        var discovery = new IntegrationDiscovery();
        var integrations = discovery.DiscoverIntegrations();

        // 2. Register each discovered integration
        foreach (var integration in integrations)
        {
            RegisterIntegration(integration);
        }

        // 3. Build subscriptions from consumer assemblies
        var subscriptionBuilder = new SubscriptionBuilder();
        var registry = new SubscriptionRegistry();

        foreach (var assembly in _builder.ConsumerAssemblies)
        {
            var subscriptions = subscriptionBuilder.BuildSubscriptions(assembly);
            registry.RegisterRange(subscriptions);

            // Register consumer types in DI
            RegisterConsumerTypes(assembly);
        }

        // 4. Register the populated registry
        _services.AddSingleton(registry);

        // 5. Register named publishers
        RegisterNamedPublishers();
    }

    private void RegisterIntegration(IntegrationDescriptor integration)
    {
        // Register the transport as keyed singleton
        _services.AddKeyedSingleton(typeof(IMessagingTransport), integration.Name, (sp, key) =>
        {
            return (IMessagingTransport)ActivatorUtilities.CreateInstance(sp, integration.TransportType);
        });

        // Register keyed publisher for this integration
        _services.AddKeyedSingleton<IMessagePublisher>(integration.Name, (sp, key) =>
        {
            var transport = sp.GetRequiredKeyedService<IMessagingTransport>(integration.Name);
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new TransportMessagePublisher(transport, serializer);
        });
    }

    private void RegisterConsumerTypes(Assembly assembly)
    {
        foreach (var type in assembly.GetExportedTypes())
        {
            var hasConsumerAttr = type.GetCustomAttributes<MessageConsumerAttribute>().Any();
            if (!hasConsumerAttr) continue;

            // Register the consumer type as transient
            _services.AddTransient(type);

            // Register all IConsumer<T> interfaces
            foreach (var iface in type.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IConsumer<>))
                {
                    _services.AddTransient(iface, type);
                }
            }
        }
    }

    private void RegisterNamedPublishers()
    {
        // Register INamedMessagePublisher
        _services.AddSingleton<INamedMessagePublisher, NamedMessagePublisher>();

        // Also register a default IMessagePublisher that delegates to the first available integration
        _services.AddSingleton<IMessagePublisher>(sp =>
        {
            var namedPublisher = sp.GetRequiredService<INamedMessagePublisher>();
            return new DefaultMessagePublisher(namedPublisher);
        });
    }
}
