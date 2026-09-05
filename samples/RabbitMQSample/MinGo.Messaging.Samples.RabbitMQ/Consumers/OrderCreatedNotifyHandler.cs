using MinGo.Messaging.Samples.RabbitMQ.Contracts;

namespace MinGo.Messaging.Samples.RabbitMQ.Consumers;

/// <summary>
/// Handles OrderCreated events for the notification service.
/// This is the second business downstream subscriber (fan-out).
/// </summary>
[MessageConsumer(Contract = typeof(OrderCreated), Group = "sample-notification")]
public sealed class OrderCreatedNotifyHandler : IConsumer<OrderCreated>
{
    private readonly ILogger<OrderCreatedNotifyHandler> _logger;

    public OrderCreatedNotifyHandler(ILogger<OrderCreatedNotifyHandler> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(ConsumeContext<OrderCreated> context, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "[Notification] Order created: {OrderId} for customer notification (Subscription: {SubscriptionId})",
            context.Message.OrderId,
            context.SubscriptionId);

        return Task.CompletedTask;
    }
}
