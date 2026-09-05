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
/// builds consumer pipelines, and auto-registers typed message bus publishers.
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

    /// <summary>
    /// Called during AddMessaging(): discovers integrations, registers subscriptions and consumers.
    /// </summary>
    public void Configure()
    {
        // 1. Discover Integration SDKs
        var discovery = new IntegrationDiscovery();
        var integrations = discovery.DiscoverIntegrations();

        // 2. Register each discovered integration as keyed services
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

        // 5. Register default IMessagePublisher when there is exactly one integration
        RegisterDefaultPublisher(integrations);
    }

    /// <summary>
    /// Called from IMessagingBuilder.AddPublishers(): scans assemblies for typed publisher
    /// interfaces decorated with <see cref="MessageBusAttribute"/> and registers them.
    /// Each typed publisher is backed by the keyed <see cref="IMessagePublisher"/> whose name
    /// matches the integration declared in <c>Messaging:Publishers:{BusName}:Integration</c>.
    /// </summary>
    public void RegisterTypedPublishers()
    {
        var publishersConfig = _configuration.GetSection("Messaging:Publishers");
        var typedPublisherTypes = DiscoverTypedPublishers();

        foreach (var (interfaceType, busName) in typedPublisherTypes)
        {
            // Resolve integration name from configuration
            var integrationName = publishersConfig
                .GetSection(busName)["Integration"];

            if (string.IsNullOrWhiteSpace(integrationName))
            {
                throw new InvalidOperationException(
                    $"No integration configured for message bus '{busName}'. " +
                    $"Add a 'Messaging:Publishers:{busName}:Integration' entry in configuration.");
            }

            // Register the typed publisher interface as a singleton factory
            _services.AddSingleton(interfaceType, sp =>
            {
                var keyedPublisher = sp.GetRequiredKeyedService<IMessagePublisher>(integrationName);
                var proxy = (TypedMessagePublisher)DispatchProxy.Create(interfaceType, typeof(TypedMessagePublisher));
                proxy.Initialize(keyedPublisher);
                return proxy;
            });
        }
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

    private void RegisterDefaultPublisher(IReadOnlyList<IntegrationDescriptor> integrations)
    {
        if (integrations.Count == 1)
        {
            // When there is exactly one integration, register its keyed publisher as the default
            var integrationName = integrations[0].Name;
            _services.AddSingleton<IMessagePublisher>(sp =>
                sp.GetRequiredKeyedService<IMessagePublisher>(integrationName));
        }
    }

    /// <summary>
    /// Scans loaded assemblies and registered consumer assemblies for interfaces
    /// decorated with <see cref="MessageBusAttribute"/> that extend <see cref="IMessagePublisher"/>.
    /// </summary>
    private IReadOnlyList<(Type InterfaceType, string BusName)> DiscoverTypedPublishers()
    {
        var result = new List<(Type, string)>();
        var seen = new HashSet<Type>();

        // Collect candidate assemblies: loaded assemblies + consumer assemblies
        var assemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Concat(_builder.ConsumerAssemblies)
            .Distinct();

        foreach (var assembly in assemblies)
        {
            try
            {
                foreach (var type in assembly.GetExportedTypes())
                {
                    if (!type.IsInterface) continue;
                    if (!typeof(IMessagePublisher).IsAssignableFrom(type)) continue;
                    if (seen.Contains(type)) continue;

                    var busAttr = type.GetCustomAttribute<MessageBusAttribute>();
                    if (busAttr is null) continue;

                    seen.Add(type);
                    result.Add((type, busAttr.Name));
                }
            }
            catch
            {
                // Skip assemblies that cannot be inspected (dynamic, reflection-only, etc.)
            }
        }

        return result;
    }
}
