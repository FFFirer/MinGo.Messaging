using MinGo.Messaging.InMemory;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MinGo.Messaging.Integration.Tests;

/// <summary>
/// Tests for the <c>UseInMemory</c> extension method that explicitly registers
/// the In-Memory integration without assembly scanning.
/// </summary>
public class UseInMemoryTests
{
    #region Service Registration

    [Fact]
    public void UseInMemory_RegistersKeyedTransport()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "InMemory"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseInMemory_RegistersKeyedPublisher()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagePublisher) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "InMemory"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseInMemory_RegistersOptions()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(InMemoryIntegrationOptions) &&
            d.Lifetime == ServiceLifetime.Singleton);

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseInMemory_RegistersSharedBus_AsSingleton()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(InMemoryBus) &&
            d.Lifetime == ServiceLifetime.Singleton);

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseInMemory_RegistersNonKeyedTransport_ForHostedService()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            !d.IsKeyedService);

        Assert.NotNull(descriptor);
    }

    #endregion

    #region Configuration Binding

    [Fact]
    public void UseInMemory_OptionsIntegrationName_IsInMemory()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseInMemory();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InMemoryIntegrationOptions>();

        Assert.Equal("InMemory", options.IntegrationName);
    }

    #endregion

    #region Builder Chaining

    [Fact]
    public void UseInMemory_ReturnsSameBuilder_ForChaining()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        var result = builder.UseInMemory();

        Assert.Same(builder, result);
    }

    [Fact]
    public void UseInMemory_NullBuilder_ThrowsArgumentNullException()
    {
        IMessagingBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.UseInMemory());
    }

    #endregion

    #region Discovery & Default Publisher

    [Fact]
    public void UseInMemory_ClaimsDiscovery_PreventsImplicitScan()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        builder.UseInMemory();

        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    [Fact]
    public async Task UseInMemory_SingleIntegration_EnablesDefaultPublisher()
    {
        var (services, config) = Setup();

        services.AddLogging();
        services.AddMessaging(config).UseInMemory();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
    }

    [Fact]
    public void UseInMemory_CalledTwice_IsIdempotent()
    {
        var (services, config) = Setup();

        services.AddMessaging(config)
            .UseInMemory()
            .UseInMemory();

        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    #endregion

    #region Typed Publisher Integration

    [Fact]
    public async Task UseInMemory_TypedPublisher_ResolvesThroughIntegration()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "InMemory"
        };

        var (services, config) = Setup(configData);

        services.AddLogging();
        services.AddMessaging(config)
            .UseInMemory()
            .AddPublishers();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<ITestOrderBusPublisher>();

        Assert.NotNull(publisher);
        Assert.IsAssignableFrom<IMessagePublisher>(publisher);
    }

    #endregion

    #region End-to-End Delivery

    [Fact]
    public async Task InMemoryTransport_PublishSubscribe_DeliversPayloadAndHeaders()
    {
        var (services, _) = Setup();
        services.AddMessaging(new ConfigurationBuilder().Build()).UseInMemory();
        services.AddLogging();

        await using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredKeyedService<IMessagingTransport>("InMemory");
        await transport.ConnectAsync();

        using var received = new SemaphoreSlim(0);
        ReadOnlyMemory<byte> capturedPayload = default;
        IDictionary<string, object>? capturedHeaders = null;

        await transport.SubscribeAsync(
            topic: "orders.created",
            subscriptionId: "sub-1",
            group: "billing",
            DeliveryMode.Competing,
            (payload, headers, ct) =>
            {
                capturedPayload = payload;
                capturedHeaders = headers;
                received.Release();
                return Task.FromResult(ConsumeResult.Ack);
            });

        var payload = new byte[] { 1, 2, 3, 4 };
        var headers = new Dictionary<string, object> { ["correlation-id"] = "abc-123" };

        await transport.PublishAsync("orders.created", payload, headers);

        Assert.True(await received.WaitAsync(TimeSpan.FromSeconds(5)), "Message was not delivered in time.");
        Assert.Equal(payload, capturedPayload.ToArray());
        Assert.NotNull(capturedHeaders);
        Assert.Equal("abc-123", capturedHeaders!["correlation-id"]);

        await transport.DisconnectAsync();
    }

    [Fact]
    public async Task InMemoryTransport_BroadcastMode_EverySubscriberReceivesMessage()
    {
        var (services, _) = Setup();
        services.AddMessaging(new ConfigurationBuilder().Build()).UseInMemory();
        services.AddLogging();

        await using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredKeyedService<IMessagingTransport>("InMemory");
        await transport.ConnectAsync();

        using var bothReceived = new SemaphoreSlim(0);
        var firstCount = 0;
        var secondCount = 0;

        await transport.SubscribeAsync(
            "config.refresh", "sub-a", "watchers",
            DeliveryMode.Broadcast,
            (_, _, _) =>
            {
                Interlocked.Increment(ref firstCount);
                bothReceived.Release();
                return Task.FromResult(ConsumeResult.Ack);
            });

        await transport.SubscribeAsync(
            "config.refresh", "sub-b", "watchers",
            DeliveryMode.Broadcast,
            (_, _, _) =>
            {
                Interlocked.Increment(ref secondCount);
                bothReceived.Release();
                return Task.FromResult(ConsumeResult.Ack);
            });

        await transport.PublishAsync("config.refresh", new byte[] { 9 }, new Dictionary<string, object>());

        // Wait for both handlers to fire.
        Assert.True(await bothReceived.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(await bothReceived.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Equal(1, firstCount);
        Assert.Equal(1, secondCount);

        await transport.DisconnectAsync();
    }

    [Fact]
    public async Task InMemoryTransport_PublishBeforeSubscribe_IsDroppedWithoutThrowing()
    {
        var (services, _) = Setup();
        services.AddMessaging(new ConfigurationBuilder().Build()).UseInMemory();
        services.AddLogging();

        await using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredKeyedService<IMessagingTransport>("InMemory");
        await transport.ConnectAsync();

        // Should not throw even when no subscriber is registered for the topic.
        await transport.PublishAsync("no.listener", new byte[] { 1 }, new Dictionary<string, object>());

        await transport.DisconnectAsync();
    }

    [Fact]
    public async Task InMemoryTransport_PublishWithoutConnect_ThrowsInvalidOperation()
    {
        var (services, _) = Setup();
        services.AddMessaging(new ConfigurationBuilder().Build()).UseInMemory();
        services.AddLogging();

        await using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredKeyedService<IMessagingTransport>("InMemory");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.PublishAsync("t", new byte[] { 1 }, new Dictionary<string, object>()));
    }

    #endregion

    #region Helpers

    private static (IServiceCollection Services, IConfiguration Configuration) Setup(
        Dictionary<string, string?>? configData = null)
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(configData ?? new Dictionary<string, string?>())
            .Build();

        return (services, config);
    }

    #endregion
}
