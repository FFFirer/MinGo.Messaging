using MinGo.Messaging.Samples.SimpleMessageBroker.Contracts;

namespace MinGo.Messaging.Samples.SimpleMessageBroker;

/// <summary>
/// Background worker that periodically publishes OrderCreated events.
/// Demonstrates the named publisher pattern.
/// </summary>
public sealed class OrderPublisherWorker : BackgroundService
{
    private readonly INamedMessagePublisher _publisher;
    private readonly ILogger<OrderPublisherWorker> _logger;

    public OrderPublisherWorker(INamedMessagePublisher publisher, ILogger<OrderPublisherWorker> logger)
    {
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for the messaging system to be fully initialized
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        var counter = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            counter++;

            var order = new OrderCreated(
                OrderId: Guid.NewGuid(),
                Product: $"Product-{counter}",
                Quantity: counter % 10 + 1);

            try
            {
                await _publisher.PublishAsync("OrderBus", order, stoppingToken);

                _logger.LogInformation(
                    "Published OrderCreated event: {OrderId} ({Product} x{Quantity})",
                    order.OrderId, order.Product, order.Quantity);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish OrderCreated event.");
            }

            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }
}
