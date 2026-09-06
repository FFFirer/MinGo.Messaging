namespace MinGo.Messaging.Tests;

/// <summary>
/// Public typed publisher interface for DI integration tests.
/// Must be public (exported) so that assembly scanning via GetExportedTypes() can discover it.
/// </summary>
[MessageBus("OrderBus")]
public interface ITestOrderBusPublisher : IMessagePublisher { }

/// <summary>
/// Second typed publisher interface for multi-publisher constructor injection tests.
/// </summary>
[MessageBus("PaymentBus")]
public interface ITestPaymentBusPublisher : IMessagePublisher { }
