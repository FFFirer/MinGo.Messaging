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

        // Guard descriptor: throws when no integrations are registered.
        // Replaced by SyncDefaultPublisher as integrations are added.
        _defaultPublisherDescriptor = ServiceDescriptor.Singleton<IMessagePublisher>(
            _ => throw new InvalidOperationException(
                "No messaging integration is registered. " +
                "Add at least one integration (e.g. UseRabbitMQ(), UseSimpleMessageBroker(), " +
                "or AddIntegration<T>()) before resolving IMessagePublisher."));
        services.Add(_defaultPublisherDescriptor);
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
    /// number of discovered integrations:
    /// <list type="number">
    ///   <item>When <c>Messaging:DefaultPublisher:Integration</c> is configured, the named integration
    ///   is used regardless of how many integrations are registered.</item>
    ///   <item>When exactly one integration is registered, it becomes the default automatically.</item>
    ///   <item>Otherwise resolving the default publisher throws an <see cref="InvalidOperationException"/>
    ///   prompting the caller to configure <c>Messaging:DefaultPublisher:Integration</c> or use a
    ///   typed publisher.</item>
    /// </list>
    /// </summary>
    private void SyncDefaultPublisher()
    {
        if (_defaultPublisherDescriptor is not null)
        {
            Services.Remove(_defaultPublisherDescriptor);
            _defaultPublisherDescriptor = null;
        }

        // 1. Explicit configuration: Messaging:DefaultPublisher:Integration
        var configuredDefault = Configuration.GetSection("Messaging:DefaultPublisher")["Integration"];

        if (!string.IsNullOrWhiteSpace(configuredDefault))
        {
            // Validation is deferred to resolve time: integrations may still be registered
            // after this call, so we cannot fail eagerly here.
            _defaultPublisherDescriptor = ServiceDescriptor.Singleton<IMessagePublisher>(
                sp =>
                {
                    var keyed = sp.GetKeyedService<IMessagePublisher>(configuredDefault);
                    if (keyed is null)
                    {
                        throw new InvalidOperationException(
                            $"The configured default publisher integration '{configuredDefault}' " +
                            $"is not registered. Available integrations: {string.Join(", ", _integrationNames)}.");
                    }

                    return keyed;
                });
            Services.Add(_defaultPublisherDescriptor);
            return;
        }

        // 2. Single integration auto-inference
        if (_integrationNames.Count == 1)
        {
            var integrationName = _integrationNames.First();
            _defaultPublisherDescriptor = ServiceDescriptor.Singleton<IMessagePublisher>(
                sp => sp.GetRequiredKeyedService<IMessagePublisher>(integrationName));
            Services.Add(_defaultPublisherDescriptor);
            return;
        }

        // 3. Ambiguous (0 or multiple integrations): register a descriptor that throws at resolve
        //    time with a clear message guiding the user toward a fix.
        _defaultPublisherDescriptor = ServiceDescriptor.Singleton<IMessagePublisher>(
            _ => throw new InvalidOperationException(
                _integrationNames.Count == 0
                    ? "No messaging integration is registered. " +
                      "Add at least one integration (e.g. UseRabbitMQ(), UseSimpleMessageBroker(), " +
                      "or AddIntegration<T>()) before resolving IMessagePublisher."
                    : $"Multiple messaging integrations are registered ({string.Join(", ", _integrationNames)}), " +
                      $"so a default IMessagePublisher cannot be inferred. " +
                      $"Set 'Messaging:DefaultPublisher:Integration' in configuration to disambiguate, " +
                      $"or use a typed publisher interface decorated with [MessageBus]."));
        Services.Add(_defaultPublisherDescriptor);
    }
}
