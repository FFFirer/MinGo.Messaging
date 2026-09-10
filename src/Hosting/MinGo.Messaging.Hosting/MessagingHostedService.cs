using MinGo.Messaging.Internal;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MinGo.Messaging;

/// <summary>
/// Hosted service that manages the messaging lifecycle:
/// connects transports on startup, subscribes consumers, and disconnects on shutdown.
/// </summary>
public sealed class MessagingHostedService : IHostedService, IAsyncDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly SubscriptionRegistry _subscriptionRegistry;
    private readonly ILogger<MessagingHostedService> _logger;
    private readonly List<IMessagingTransport> _transports = new();
    private readonly CancellationTokenSource _cts = new();

    public MessagingHostedService(
        IServiceProvider serviceProvider,
        SubscriptionRegistry subscriptionRegistry,
        ILogger<MessagingHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _subscriptionRegistry = subscriptionRegistry;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting messaging hosted service...");

        // ── Connect all registered transports ────────────────────────────────────────────────────
        var transports = _serviceProvider.GetServices<IMessagingTransport>();
        foreach (var transport in transports)
        {
            await transport.ConnectAsync(cancellationToken);
            _transports.Add(transport);
            _logger.LogInformation("Transport connected: {TransportType}", transport.GetType().Name);
        }

        if (_transports.Count == 0)
        {
            _logger.LogWarning(
                "No messaging transports are registered. " +
                "Consumers will not be subscribed. Register at least one integration " +
                "(e.g. UseRabbitMQ(), UseSimpleMessageBroker()).");
            return;
        }

        // ── Select the transport used for all consumer subscriptions ───────────────────────────
        // When multiple transports are registered, the first one is used as the default consumer
        // transport. Per-subscription transport routing can be added via configuration in the future.
        var consumerTransport = _transports[0];

        if (_transports.Count > 1)
        {
            _logger.LogWarning(
                "Multiple transports are registered ({Count}). " +
                "All consumer subscriptions will use '{TransportType}'. " +
                "Per-subscription transport routing is not yet configurable.",
                _transports.Count,
                consumerTransport.GetType().Name);
        }

        // ── Subscribe all discovered subscriptions ─────────────────────────────────────────────
        var dispatcher = _serviceProvider.GetRequiredService<ConsumerDispatcher>();

        foreach (var subscription in _subscriptionRegistry.Subscriptions)
        {
            var handler = dispatcher.CreateHandler(subscription);

            await consumerTransport.SubscribeAsync(
                topic: subscription.ContractId,
                subscriptionId: subscription.Id,
                group: subscription.Target.ConsumerGroupId ?? subscription.ConsumerServiceId,
                deliveryMode: subscription.DeliveryMode,
                handler: handler,
                cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Subscribed: {SubscriptionId} | topic={Topic} | group={Group} | mode={DeliveryMode}",
                subscription.Id,
                subscription.ContractId,
                subscription.Target.ConsumerGroupId ?? subscription.ConsumerServiceId,
                subscription.DeliveryMode);
        }

        _logger.LogInformation(
            "Messaging hosted service started with {TransportCount} transport(s) " +
            "and {Count} subscription(s).",
            _transports.Count,
            _subscriptionRegistry.Subscriptions.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping messaging hosted service...");

        _cts.Cancel();

        // Disconnect all transports
        foreach (var transport in _transports)
        {
            try
            {
                await transport.DisconnectAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disconnecting transport.");
            }
        }

        _logger.LogInformation("Messaging hosted service stopped.");
    }

    public ValueTask DisposeAsync()
    {
        _cts.Dispose();
        return ValueTask.CompletedTask;
    }
}
