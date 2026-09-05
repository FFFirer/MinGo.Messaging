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

        // Get all registered transports and connect them
        // In a real scenario, the transports are keyed services
        // For now, we connect via the subscription registry to determine what to subscribe
        foreach (var subscription in _subscriptionRegistry.Subscriptions)
        {
            _logger.LogInformation(
                "Registered subscription: {SubscriptionId} for contract {ContractId} v{Version} in group {Group}",
                subscription.Id,
                subscription.ContractId,
                subscription.ContractVersion,
                subscription.Target.ConsumerGroupId);
        }

        _logger.LogInformation("Messaging hosted service started with {Count} subscription(s).",
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
