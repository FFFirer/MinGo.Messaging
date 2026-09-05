namespace MinGo.Messaging.Samples.RabbitMQ.Contracts;

/// <summary>
/// Event raised when a new order is created.
/// </summary>
[MessageContract(Id = "sample.order.created", Version = "1", Kind = MessageKind.Event)]
public sealed record OrderCreated(
    Guid OrderId,
    string Product,
    int Quantity) : IEvent;
