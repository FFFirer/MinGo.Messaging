using MinGo.Messaging.Samples.RabbitMQ.Contracts;

namespace MinGo.Messaging.Samples.RabbitMQ.Consumers;

/// <summary>
/// Handles OrderCreated events for the billing service.
/// This is one of two business downstream subscribers (fan-out).
/// </summary>
[MessageConsumer(Contract = typeof(OrderCreated), Group = "sample-billing")]
public sealed class OrderCreatedBillingHandler : IConsumer<OrderCreated>
{
    private readonly ILogger<OrderCreatedBillingHandler> _logger;

    public OrderCreatedBillingHandler(ILogger<OrderCreatedBillingHandler> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(ConsumeContext<OrderCreated> context, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "[Billing] Processing order {OrderId}: {Product} x{Quantity} (Subscription: {SubscriptionId})",
            context.Message.OrderId,
            context.Message.Product,
            context.Message.Quantity,
            context.SubscriptionId);

        return Task.CompletedTask;
    }
}
