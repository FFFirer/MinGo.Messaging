using MinGo.Messaging.Internal;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace MinGo.Messaging.Tests;

/// <summary>
/// Tests for constructor injection patterns of IMessagePublisher and typed publishers.
/// Verifies that DI correctly resolves publishers into consumer services,
/// matching real-world usage like OrderPublisherWorker(IOrderBusPublisher).
/// </summary>
public class PublisherConstructorInjectionTests
{
    #region Default IMessagePublisher Constructor Injection

    [Fact]
    public async Task DefaultPublisher_InjectedIntoTransientService()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");
        services.AddTransient<OrderService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<OrderService>();

        Assert.NotNull(service.Publisher);
        Assert.IsType<TransportMessagePublisher>(service.Publisher);
    }

    [Fact]
    public async Task DefaultPublisher_SameInstanceInjectedAcrossMultipleServices()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");
        services.AddTransient<OrderService>();

        await using var sp = services.BuildServiceProvider();
        var service1 = sp.GetRequiredService<OrderService>();
        var service2 = sp.GetRequiredService<OrderService>();

        // Publisher is singleton — both services get the same instance
        Assert.Same(service1.Publisher, service2.Publisher);
    }

    [Fact]
    public void DefaultPublisher_ThrowsWhenNoIntegrationRegistered()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config);
        services.AddTransient<OrderService>();

        using var sp = services.BuildServiceProvider();

        // OrderService requires IMessagePublisher, but none is registered
        Assert.Throws<InvalidOperationException>(() => sp.GetRequiredService<OrderService>());
    }

    #endregion

    #region Typed Publisher Constructor Injection

    [Fact]
    public async Task TypedPublisher_InjectedIntoTransientService()
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
        services.AddTransient<TypedOrderService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<TypedOrderService>();

        Assert.NotNull(service.Publisher);
        Assert.IsAssignableFrom<ITestOrderBusPublisher>(service.Publisher);
        Assert.IsAssignableFrom<IMessagePublisher>(service.Publisher);
    }

    [Fact]
    public async Task TypedPublisher_InjectedInstanceIsSingleton()
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
        services.AddTransient<TypedOrderService>();

        await using var sp = services.BuildServiceProvider();
        var service1 = sp.GetRequiredService<TypedOrderService>();
        var service2 = sp.GetRequiredService<TypedOrderService>();

        // Typed publisher is registered as singleton — same proxy instance
        Assert.Same(service1.Publisher, service2.Publisher);
    }

    #endregion

    #region Keyed Publisher Constructor Injection ([FromKeyedServices])

    [Fact]
    public async Task KeyedPublisher_InjectedViaFromKeyedServicesAttribute()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ");
        services.AddTransient<KeyedPublisherService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<KeyedPublisherService>();

        Assert.NotNull(service.Publisher);
        Assert.IsType<TransportMessagePublisher>(service.Publisher);
    }

    [Fact]
    public async Task KeyedPublisher_InjectsCorrectIntegration()
    {
        var (services, config) = Setup(publisherConfig: null);

        services.AddMessaging(config)
            .AddIntegration<FakeTransport>("RabbitMQ")
            .AddIntegration<FakeTransport>("SMB");
        services.AddTransient<KeyedPublisherService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<KeyedPublisherService>();

        // [FromKeyedServices("RabbitMQ")] should resolve the RabbitMQ-keyed publisher
        Assert.NotNull(service.Publisher);
        var directKeyed = sp.GetKeyedService<IMessagePublisher>("RabbitMQ");
        Assert.Same(directKeyed, service.Publisher);
    }

    #endregion

    #region Multiple Publishers in One Constructor

    [Fact]
    public async Task MultipleTypedPublishers_InjectedIntoSameService()
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
        services.AddTransient<MultiPublisherService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<MultiPublisherService>();

        Assert.NotNull(service.OrderPublisher);
        Assert.NotNull(service.PaymentPublisher);
        Assert.IsAssignableFrom<ITestOrderBusPublisher>(service.OrderPublisher);
        Assert.IsAssignableFrom<ITestPaymentBusPublisher>(service.PaymentPublisher);
        // Different typed publishers are distinct proxy instances
        Assert.NotSame(service.OrderPublisher, service.PaymentPublisher);
    }

    [Fact]
    public async Task DefaultAndTypedPublisher_InjectedTogether()
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
        services.AddTransient<MixedPublisherService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<MixedPublisherService>();

        Assert.NotNull(service.DefaultPublisher);
        Assert.NotNull(service.TypedPublisher);
        // Both ultimately delegate to the same keyed publisher
        Assert.IsType<TransportMessagePublisher>(service.DefaultPublisher);
        Assert.IsAssignableFrom<ITestOrderBusPublisher>(service.TypedPublisher);
    }

    #endregion

    #region End-to-End: Injected Publisher Publishes Through Transport

    [Fact]
    public async Task InjectedDefaultPublisher_PublishesThroughTransport()
    {
        var transportMock = new Mock<IMessagingTransport>();
        transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var (services, config) = Setup(publisherConfig: null);

        // Manually register to control the mock
        services.AddKeyedSingleton<IMessagingTransport>("RabbitMQ", transportMock.Object);
        services.AddSingleton<IMessageSerializer>(new FakeSerializer());
        services.AddKeyedSingleton<IMessagePublisher>("RabbitMQ", (sp, key) =>
            new Internal.TransportMessagePublisher(
                sp.GetRequiredKeyedService<IMessagingTransport>("RabbitMQ"),
                sp.GetRequiredService<IMessageSerializer>()));
        // Register default (non-keyed) publisher pointing to the same keyed one
        services.AddSingleton<IMessagePublisher>(sp =>
            sp.GetRequiredKeyedService<IMessagePublisher>("RabbitMQ"));

        services.AddTransient<OrderService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<OrderService>();
        var message = new TestOrderEvent();

        await service.PublishOrderAsync(message);

        transportMock.Verify(
            t => t.PublishAsync(
                "test.order.event",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task InjectedTypedPublisher_PublishesThroughTransport()
    {
        var transportMock = new Mock<IMessagingTransport>();
        transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "RabbitMQ",
            ["Messaging:Publishers:PaymentBus:Integration"] = "RabbitMQ"
        };

        var (services, config) = Setup(publisherConfig: configData);

        // Manually register keyed services with mock transport
        services.AddKeyedSingleton<IMessagingTransport>("RabbitMQ", transportMock.Object);
        services.AddSingleton<IMessageSerializer>(new FakeSerializer());
        services.AddKeyedSingleton<IMessagePublisher>("RabbitMQ", (sp, key) =>
            new Internal.TransportMessagePublisher(
                sp.GetRequiredKeyedService<IMessagingTransport>("RabbitMQ"),
                sp.GetRequiredService<IMessageSerializer>()));

        // Use AddPublishers() only for typed publisher discovery
        var builder = new MessagingBuilder(services, config);
        builder.AddPublishers();

        services.AddTransient<TypedOrderService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<TypedOrderService>();
        var message = new TestOrderEvent();

        await service.PublishOrderAsync(message);

        transportMock.Verify(
            t => t.PublishAsync(
                "test.order.event",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    #endregion

    #region Injected Publisher — SendAsync via Constructor

    [Fact]
    public async Task InjectedDefaultPublisher_SendAsyncWorksThroughTransport()
    {
        var transportMock = new Mock<IMessagingTransport>();
        transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var (services, config) = Setup(publisherConfig: null);

        services.AddKeyedSingleton<IMessagingTransport>("RabbitMQ", transportMock.Object);
        services.AddSingleton<IMessageSerializer>(new FakeSerializer());
        services.AddKeyedSingleton<IMessagePublisher>("RabbitMQ", (sp, key) =>
            new Internal.TransportMessagePublisher(
                sp.GetRequiredKeyedService<IMessagingTransport>("RabbitMQ"),
                sp.GetRequiredService<IMessageSerializer>()));
        services.AddSingleton<IMessagePublisher>(sp =>
            sp.GetRequiredKeyedService<IMessagePublisher>("RabbitMQ"));

        services.AddTransient<OrderService>();

        await using var sp = services.BuildServiceProvider();
        var service = sp.GetRequiredService<OrderService>();
        var message = new TestOrderEvent();

        await service.SendCommandAsync(message);

        transportMock.Verify(
            t => t.PublishAsync(
                "test.order.event",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
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

    #region Service Classes Under Test

    /// <summary>
    /// Service that takes the default (non-keyed) IMessagePublisher via constructor injection.
    /// </summary>
    private sealed class OrderService
    {
        private readonly IMessagePublisher _publisher;

        public OrderService(IMessagePublisher publisher)
        {
            _publisher = publisher;
        }

        public IMessagePublisher Publisher => _publisher;

        public Task PublishOrderAsync(IMessage message, CancellationToken ct = default)
            => _publisher.PublishAsync(message, ct);

        public Task SendCommandAsync(IMessage message, CancellationToken ct = default)
            => _publisher.SendAsync(message, ct);
    }

    /// <summary>
    /// Service that takes a typed publisher (IOrderBusPublisher) via constructor injection.
    /// Mirrors the OrderPublisherWorker pattern.
    /// </summary>
    private sealed class TypedOrderService
    {
        private readonly ITestOrderBusPublisher _publisher;

        public TypedOrderService(ITestOrderBusPublisher publisher)
        {
            _publisher = publisher;
        }

        public ITestOrderBusPublisher Publisher => _publisher;

        public Task PublishOrderAsync(IMessage message, CancellationToken ct = default)
            => _publisher.PublishAsync(message, ct);
    }

    /// <summary>
    /// Service that uses [FromKeyedServices] to inject a specific keyed publisher.
    /// </summary>
    private sealed class KeyedPublisherService
    {
        private readonly IMessagePublisher _publisher;

        public KeyedPublisherService(
            [FromKeyedServices("RabbitMQ")] IMessagePublisher publisher)
        {
            _publisher = publisher;
        }

        public IMessagePublisher Publisher => _publisher;
    }

    /// <summary>
    /// Service that takes multiple typed publishers via constructor injection.
    /// </summary>
    private sealed class MultiPublisherService
    {
        private readonly ITestOrderBusPublisher _orderPublisher;
        private readonly ITestPaymentBusPublisher _paymentPublisher;

        public MultiPublisherService(
            ITestOrderBusPublisher orderPublisher,
            ITestPaymentBusPublisher paymentPublisher)
        {
            _orderPublisher = orderPublisher;
            _paymentPublisher = paymentPublisher;
        }

        public ITestOrderBusPublisher OrderPublisher => _orderPublisher;
        public ITestPaymentBusPublisher PaymentPublisher => _paymentPublisher;
    }

    /// <summary>
    /// Service that takes both default IMessagePublisher and a typed publisher.
    /// </summary>
    private sealed class MixedPublisherService
    {
        private readonly IMessagePublisher _defaultPublisher;
        private readonly ITestOrderBusPublisher _typedPublisher;

        public MixedPublisherService(
            IMessagePublisher defaultPublisher,
            ITestOrderBusPublisher typedPublisher)
        {
            _defaultPublisher = defaultPublisher;
            _typedPublisher = typedPublisher;
        }

        public IMessagePublisher DefaultPublisher => _defaultPublisher;
        public ITestOrderBusPublisher TypedPublisher => _typedPublisher;
    }

    #endregion
}
