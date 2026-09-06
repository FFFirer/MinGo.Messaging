namespace MinGo.Messaging.Integration.Tests;

/// <summary>
/// Public typed publisher interface for integration tests.
/// Must be public (exported) so that assembly scanning via GetExportedTypes() can discover it.
/// </summary>
[MessageBus("OrderBus")]
public interface ITestOrderBusPublisher : IMessagePublisher { }
