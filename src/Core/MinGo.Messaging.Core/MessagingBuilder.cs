using System.Reflection;
using MinGo.Messaging.Integration;
using MinGo.Messaging.Internal;
using MinGo.Messaging.Pipeline;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging;

/// <summary>
/// Default implementation of <see cref="IMessagingBuilder"/>.
/// </summary>
/// <remarks>
/// Discovery is driven by the fluent chain: each <c>AddIntegrations</c>/<c>AddPublishers</c>/
/// <c>AddConsumer</c> call resolves the assemblies to scan (optionally narrowed by an
/// <see cref="AssemblyName"/> filter over the application dependency graph) and registers the
/// results immediately. Scan/de-dup state lives here so repeated calls combine as a union.
/// </remarks>
internal sealed class MessagingBuilder : IMessagingBuilder
{
    private static readonly Func<AssemblyName, bool>[] NoFilters = [];

    private readonly AssemblyScanResolver _resolver = new();
    private readonly MessagingAutoConfigurer _registrar;
    private readonly SubscriptionRegistry _registry = new();

    // Assemblies registered for consumers; also scanned for typed publishers so that publisher
    // interfaces declared alongside handlers are found regardless of chaining order.
    private readonly List<Assembly> _consumerAssemblies = new();

    // De-duplication state (survives across repeated Add* calls).
    private readonly HashSet<string> _scannedConsumerAssemblies = new(StringComparer.Ordinal);
    private readonly HashSet<string> _integrationNames = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<Type> _registeredPublisherTypes = new();

    private ServiceDescriptor? _defaultPublisherDescriptor;
    private bool _integrationsDiscovered;

    public MessagingBuilder(IServiceCollection services, IConfiguration configuration)
    {
        Services = services;
        Configuration = configuration;
        _registrar = new MessagingAutoConfigurer(services, configuration);

        // The registry is a single mutable singleton, populated as consumers are added.
        services.AddSingleton(_registry);
    }

    public IServiceCollection Services { get; }

    public IConfiguration Configuration { get; }

    public IMessagingBuilder AddPublishers()
    {
        EnsureIntegrationsDiscovered();
        RegisterTypedPublishers(_resolver.ResolveAll());
        return this;
    }

    public IMessagingBuilder AddPublishers(Func<AssemblyName, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        EnsureIntegrationsDiscovered();
        RegisterTypedPublishers(_resolver.Resolve(filter));
        return this;
    }

    public IMessagingBuilder AddIntegrations(Func<AssemblyName, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);

        // Claim discovery so the implicit default (whole-graph) scan does not also run.
        _integrationsDiscovered = true;
        DiscoverIntegrations(_resolver.Resolve(filter));
        return this;
    }

    public IMessagingBuilder AddIntegration<TTransport>(string name)
        where TTransport : class, IMessagingTransport
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        // Explicit registration claims discovery so the implicit default scan will not also run.
        _integrationsDiscovered = true;

        if (_integrationNames.Add(name))
        {
            _registrar.RegisterIntegrationServices(name, typeof(TTransport));
            SyncDefaultPublisher();
        }

        return this;
    }

    public IMessagingBuilder AddConsumer(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        EnsureIntegrationsDiscovered();
        RegisterConsumers(assembly);
        return this;
    }

    public IMessagingBuilder AddConsumer(Func<AssemblyName, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        EnsureIntegrationsDiscovered();

        foreach (var assembly in _resolver.Resolve(filter))
        {
            RegisterConsumers(assembly);
        }

        return this;
    }

    public IMessagingBuilder ConfigureSerializer(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        Services.AddSingleton<IMessageSerializer>(serializer);
        return this;
    }

    public IMessagingBuilder AddPipelineMiddleware<TMiddleware>() where TMiddleware : class, IConsumerPipelineMiddleware
    {
        Services.AddSingleton<IConsumerPipelineMiddleware, TMiddleware>();
        return this;
    }

    /// <summary>
    /// Ensures integrations are discovered exactly once. When the caller never used
    /// <see cref="AddIntegrations"/>, the default whole-dependency-graph scan runs here so that
    /// publishers/consumers always have their transports available.
    /// </summary>
    private void EnsureIntegrationsDiscovered()
    {
        if (_integrationsDiscovered) return;
        _integrationsDiscovered = true;
        DiscoverIntegrations(_resolver.Resolve(NoFilters));
    }

    private void DiscoverIntegrations(IReadOnlyList<Assembly> assemblies)
    {
        foreach (var descriptor in IntegrationDiscovery.FindIntegrations(assemblies))
        {
            // De-duplicate by integration name across repeated scans.
            if (!_integrationNames.Add(descriptor.Name)) continue;

            _registrar.RegisterDiscoveredIntegration(descriptor);
        }

        SyncDefaultPublisher();
    }

    private void RegisterConsumers(Assembly assembly)
    {
        var key = assembly.FullName ?? assembly.GetName().Name ?? assembly.ToString();
        if (!_scannedConsumerAssemblies.Add(key)) return;

        _consumerAssemblies.Add(assembly);
        _registrar.RegisterConsumers(assembly, _registry);
    }

    private void RegisterTypedPublishers(IReadOnlyList<Assembly> assemblies)
    {
        // Union with consumer assemblies so publisher interfaces declared next to handlers are found.
        IEnumerable<Assembly> candidates = assemblies;
        if (_consumerAssemblies.Count > 0)
        {
            candidates = candidates.Concat(_consumerAssemblies);
        }

        _registrar.RegisterTypedPublishers(candidates, _registeredPublisherTypes);
    }

    /// <summary>
    /// Keeps the default (non-keyed) <see cref="IMessagePublisher"/> registration in sync with the
    /// number of discovered integrations: it is present only when there is exactly one integration.
    /// </summary>
    private void SyncDefaultPublisher()
    {
        if (_defaultPublisherDescriptor is not null)
        {
            Services.Remove(_defaultPublisherDescriptor);
            _defaultPublisherDescriptor = null;
        }

        if (_integrationNames.Count != 1) return;

        // Exactly one integration: expose it as the default (non-keyed) publisher too.
        var integrationName = _integrationNames.First();
        _defaultPublisherDescriptor = ServiceDescriptor.Singleton<IMessagePublisher>(
            sp => sp.GetRequiredKeyedService<IMessagePublisher>(integrationName));
        Services.Add(_defaultPublisherDescriptor);
    }
}
