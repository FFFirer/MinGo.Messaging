using MinGo.Messaging.Internal;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace MinGo.Messaging.Tests;

/// <summary>
/// Integration tests for IMessagePublisher DI registration patterns:
/// default (non-keyed), keyed, and typed publisher resolution.
/// </summary>
public class PublisherRegistrationTests
{
    #region Default (Non-Keyed) Publisher

    [Fact]
    public async Task SingleIntegration_ResolvesDefaultPublisher()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
        Assert.IsType<TransportMessagePublisher>(publisher);
    }

    [Fact]
    public async Task NoIntegrations_DefaultPublisher_Throws()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config);

        await using var sp = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            sp.GetService<IMessagePublisher>());

        Assert.Contains("No messaging integration is registered", ex.Message);
    }

    [Fact]
    public async Task MultipleIntegrations_DefaultPublisher_Throws()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddIntegration<FakeTransport>("SMB");

        await using var sp = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            sp.GetService<IMessagePublisher>());

        Assert.Contains("Multiple messaging integrations", ex.Message);
        Assert.Contains("DefaultPublisher", ex.Message);
    }

    #endregion

    #region Default Publisher via Configuration

    [Fact]
    public async Task MultipleIntegrations_WithDefaultPublisherConfig_ResolvesConfiguredIntegration()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:DefaultPublisher:Integration"] = "SMB"
        };

        var (services, config) = Setup(publisherConfig: configData);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddIntegration<FakeTransport>("SMB");

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
        // The default publisher should delegate to the "SMB" keyed publisher
        var smbPublisher = sp.GetKeyedService<IMessagePublisher>("SMB");
        Assert.Same(smbPublisher, publisher);
    }

    [Fact]
    public async Task DefaultPublisherConfig_NonExistentIntegration_ThrowsAtResolveTime()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:DefaultPublisher:Integration"] = "NonExistent"
        };

        var (services, config) = Setup(publisherConfig: configData);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        // Registration succeeds; the error surfaces when the default publisher is resolved.
        await using var sp = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            sp.GetService<IMessagePublisher>());

        Assert.Contains("NonExistent", ex.Message);
        Assert.Contains("not registered", ex.Message);
    }

    [Fact]
    public async Task SingleIntegration_WithDefaultPublisherConfig_UsesConfiguredIntegration()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:DefaultPublisher:Integration"] = "RabbitMQ"
        };

        var (services, config) = Setup(publisherConfig: configData);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
        var rabbitPublisher = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");
        Assert.Same(rabbitPublisher, publisher);
    }

    #endregion

    #region Keyed Publisher

    [Fact]
    public async Task KeyedPublisher_CanBeResolvedByName()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");

        Assert.NotNull(publisher);
        Assert.IsType<TransportMessagePublisher>(publisher);
    }

    [Fact]
    public async Task DifferentIntegrations_ProduceDifferentKeyedPublishers()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddIntegration<FakeTransport>("SMB");

        await using var sp = services.BuildServiceProvider();
        var rabbitPublisher = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");
        var smbPublisher = sp.GetKeyedService<IMessagePublisher>("SMB");

        Assert.NotNull(rabbitPublisher);
        Assert.NotNull(smbPublisher);
        Assert.NotSame(rabbitPublisher, smbPublisher);
    }

    [Fact]
    public async Task NonExistentKey_ReturnsNull()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetKeyedService<IMessagePublisher>("NonExistent");

        Assert.Null(publisher);
    }

    #endregion

    #region Typed Publisher (DispatchProxy via DI)

    [Fact]
    public async Task TypedPublisher_ResolvesFromDI_WhenConfigured()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "RabbitMQ",
            ["Messaging:Publishers:PaymentBus:Integration"] = "RabbitMQ"
        };

        var (services, config) = Setup(publisherConfig: configData);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddPublishers();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<ITestOrderBusPublisher>();

        Assert.NotNull(publisher);
        Assert.IsAssignableFrom<IMessagePublisher>(publisher);
    }

    [Fact]
    public async Task TypedPublisher_DelegatesToKeyedPublisher()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "RabbitMQ",
            ["Messaging:Publishers:PaymentBus:Integration"] = "RabbitMQ"
        };

        var transportMock = new Mock<IMessagingTransport>();
        transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var (services, config) = Setup(publisherConfig: configData);

        // Manually register keyed services so we control the transport mock
        services.AddKeyedSingleton<IMessagingTransport>("RabbitMQ", transportMock.Object);
        services.AddSingleton<IMessageSerializer>(new FakeSerializer());
        services.AddKeyedSingleton<IMessagePublisher>("RabbitMQ", (sp, key) =>
        {
            var transport = sp.GetRequiredKeyedService<IMessagingTransport>("RabbitMQ");
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new Internal.TransportMessagePublisher(transport, serializer);
        });

        // Only use AddPublishers() for typed publisher discovery (not AddIntegration)
        var builder = new MessagingBuilder(services, config);
        builder.AddPublishers();

        await using var sp = services.BuildServiceProvider();
        var typedPublisher = sp.GetRequiredService<ITestOrderBusPublisher>();
        var message = new TestOrderEvent();

        await typedPublisher.PublishAsync(message);

        transportMock.Verify(
            t => t.PublishAsync(
                "test.order.event",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public void TypedPublisher_ThrowsWhenNoIntegrationConfigured()
    {
        var configData = new Dictionary<string, string?>();

        var (services, config) = Setup(publisherConfig: configData);

        var builder = services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        Assert.Throws<InvalidOperationException>(() => builder.AddPublishers());
    }

    #endregion

    #region Duplicate Integration Registration

    [Fact]
    public async Task AddIntegration_SameNameTwice_IsIdempotent()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        // Should still resolve default publisher (count == 1, not 2)
        var publisher = sp.GetService<IMessagePublisher>();
        Assert.NotNull(publisher);
    }

    #endregion

    #region Publisher Singleton Behavior

    [Fact]
    public async Task KeyedPublisher_IsSingleton()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher1 = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");
        var publisher2 = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");

        Assert.Same(publisher1, publisher2);
    }

    [Fact]
    public async Task DefaultPublisher_IsSingleton()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");

        await using var sp = services.BuildServiceProvider();
        var publisher1 = sp.GetService<IMessagePublisher>();
        var publisher2 = sp.GetService<IMessagePublisher>();

        Assert.Same(publisher1, publisher2);
    }

    #endregion

    #region Helpers

    private static (IServiceCollection Services, IConfiguration Configuration) Setup(
        Dictionary<string, string?>? publisherConfig)
    {
        var services = new ServiceCollection();
        var configData = publisherConfig ?? new Dictionary<string, string?>();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        return (services, config);
    }

    #endregion

    #region Test Types

    /// <summary>
    /// Minimal transport for DI registration tests.
    /// </summary>
    private class FakeTransport : IMessagingTransport
    {
        public Task ConnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(string topic, ReadOnlyMemory<byte> data, IDictionary<string, object> headers, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(string topic, string subscriptionId, string group, Subscriptions.DeliveryMode deliveryMode, Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<Transport.ConsumeResult>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DisconnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class FakeSerializer : IMessageSerializer
    {
        public ReadOnlyMemory<byte> Serialize(IMessage message, Type messageType) => new byte[] { 1 };
        public IMessage? Deserialize(ReadOnlyMemory<byte> data, Type messageType) => null;
    }

    [MessageContract(Id = "test.order.event", Version = "1", Kind = MessageKind.Event)]
    private sealed record TestOrderEvent : IEvent;

    #endregion
}
