namespace MinGo.Messaging.Samples.RabbitMQ;

/// <summary>
/// Typed message bus publisher for order-related messages.
/// The <see cref="MessageBusAttribute"/> binds this interface to the "OrderBus" logical bus,
/// whose transport is resolved from configuration (Messaging:Publishers:OrderBus:Integration).
/// </summary>
[MessageBus("OrderBus")]
public interface IOrderBusPublisher : IMessagePublisher { }
