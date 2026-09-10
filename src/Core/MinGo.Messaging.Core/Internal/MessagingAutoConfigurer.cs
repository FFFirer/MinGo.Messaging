using System.Reflection;
using MinGo.Messaging.Integration;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Performs the convention-driven DI registration for the messaging system: registers discovered
/// Integration SDKs as keyed services, builds consumer subscriptions, registers consumer types,
/// and registers typed message bus publishers.
/// </summary>
/// <remarks>
/// This type is stateless with respect to which assemblies have already been processed; the caller
/// (<see cref="MessagingBuilder"/>) owns the scan/dedup state and passes in the assemblies to act on.
/// </remarks>
internal sealed class MessagingAutoConfigurer
{
    private readonly IServiceCollection _services;
    private readonly IConfiguration _configuration;

    public MessagingAutoConfigurer(IServiceCollection services, IConfiguration configuration)
    {
        _services = services;
        _configuration = configuration;
    }

    /// <summary>
    /// Registers a discovered integration (assembly-scanning path): invokes its convention-based
    /// Configure hook to bind options / register SDK services, then registers the keyed services.
    /// </summary>
    public void RegisterDiscoveredIntegration(IntegrationDescriptor integration)
    {
        // Let the integration SDK register its own dependencies (options, client SDKs, etc.)
        ConfigureIntegration(integration);

        RegisterIntegrationServices(integration.Name, integration.TransportType);
    }

    /// <summary>
    /// Registers the keyed <see cref="IMessagingTransport"/> and keyed <see cref="IMessagePublisher"/>
    /// for an integration. This does NOT bind options or invoke any Configure hook — the caller must
    /// have registered whatever services the transport's constructor requires. Used by both the
    /// explicit (<c>AddIntegration</c>) and scanning paths.
    /// </summary>
    public void RegisterIntegrationServices(string name, Type transportType)
    {
        // Register the transport as keyed singleton
        _services.AddKeyedSingleton(typeof(IMessagingTransport), name, (sp, key) =>
        {
            return (IMessagingTransport)ActivatorUtilities.CreateInstance(sp, transportType);
        });

        // Also register as non-keyed so the hosted service can resolve all transports
        // via IEnumerable<IMessagingTransport> for lifecycle management (connect/disconnect).
        _services.AddSingleton<IMessagingTransport>(sp =>
            (IMessagingTransport)sp.GetRequiredKeyedService<IMessagingTransport>(name));

        // Register keyed publisher for this integration
        _services.AddKeyedSingleton<IMessagePublisher>(name, (sp, key) =>
        {
            var transport = sp.GetRequiredKeyedService<IMessagingTransport>(name);
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new TransportMessagePublisher(transport, serializer);
        });
    }

    /// <summary>
    /// Builds subscriptions for the consumers declared in <paramref name="assembly"/>, adds them to
    /// the shared <paramref name="registry"/>, and registers the consumer types in DI.
    /// </summary>
    public void RegisterConsumers(Assembly assembly, SubscriptionRegistry registry)
    {
        var subscriptions = new SubscriptionBuilder().BuildSubscriptions(assembly);
        registry.RegisterRange(subscriptions);

        RegisterConsumerTypes(assembly);
    }

    /// <summary>
    /// Scans the supplied assemblies for interfaces decorated with <see cref="MessageBusAttribute"/>
    /// that extend <see cref="IMessagePublisher"/> and registers each as a singleton backed by the
    /// keyed publisher whose name matches <c>Messaging:Publishers:{BusName}:Integration</c>.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <param name="registeredTypes">
    /// Accumulator of already-registered publisher interfaces, used to deduplicate across scans.
    /// </param>
    public void RegisterTypedPublishers(IEnumerable<Assembly> assemblies, HashSet<Type> registeredTypes)
    {
        var publishersConfig = _configuration.GetSection("Messaging:Publishers");

        foreach (var (interfaceType, busName) in DiscoverTypedPublishers(assemblies, registeredTypes))
        {
            // Resolve integration name from configuration
            var integrationName = publishersConfig.GetSection(busName)["Integration"];

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

    /// <summary>
    /// Invokes the integration's static Configure(IServiceCollection, IConfiguration) method
    /// if one exists, allowing the SDK to register its own dependencies.
    /// </summary>
    private void ConfigureIntegration(IntegrationDescriptor integration)
    {
        var section = _configuration.GetSection($"Messaging:Integrations:{integration.Name}");
        var registrationType = FindRegistrationType(integration.Assembly);

        if (registrationType is not null)
        {
            registrationType.GetMethod("Configure", [typeof(IServiceCollection), typeof(IConfiguration)])
                ?.Invoke(null, [_services, section]);
        }
    }

    private static Type? FindRegistrationType(Assembly assembly)
    {
        foreach (var type in assembly.GetExportedTypes())
        {
            if (!type.IsAbstract || !type.IsSealed) continue; // static class check

            var method = type.GetMethod("Configure", BindingFlags.Public | BindingFlags.Static,
                null, [typeof(IServiceCollection), typeof(IConfiguration)], null);

            if (method is not null) return type;
        }

        return null;
    }

    private void RegisterConsumerTypes(Assembly assembly)
    {
        foreach (var type in assembly.GetExportedTypes())
        {
            var hasConsumerAttr = type.GetCustomAttributes<MessageConsumerAttribute>().Any();
            if (!hasConsumerAttr) continue;

            // Register the consumer type as scoped.
            // Each message delivery is processed inside its own DI scope (created by
            // ConsumerDispatcher), so a scoped consumer — and any scoped dependencies it
            // requires (e.g. DbContext, unit-of-work) — is instantiated per message and
            // disposed when that message finishes processing.
            _services.AddScoped(type);

            // Register all IConsumer<T> interfaces with the same scoped lifetime so that
            // resolving the interface within a message scope yields the scoped instance.
            foreach (var iface in type.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IConsumer<>))
                {
                    _services.AddScoped(iface, type);
                }
            }
        }
    }

    private static IEnumerable<(Type InterfaceType, string BusName)> DiscoverTypedPublishers(
        IEnumerable<Assembly> assemblies, HashSet<Type> registeredTypes)
    {
        foreach (var assembly in assemblies)
        {
            Type[] types;
            try
            {
                types = assembly.GetExportedTypes();
            }
            catch
            {
                // Skip assemblies that cannot be inspected (dynamic, reflection-only, etc.)
                continue;
            }

            foreach (var type in types)
            {
                if (!type.IsInterface) continue;
                if (!typeof(IMessagePublisher).IsAssignableFrom(type)) continue;

                var busAttr = type.GetCustomAttribute<MessageBusAttribute>();
                if (busAttr is null) continue;

                // Deduplicate across repeated scans (a type may be reachable from multiple assemblies).
                if (!registeredTypes.Add(type)) continue;

                yield return (type, busAttr.Name);
            }
        }
    }
}
