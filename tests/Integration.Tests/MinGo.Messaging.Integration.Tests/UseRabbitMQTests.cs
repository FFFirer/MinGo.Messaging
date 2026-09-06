using MinGo.Messaging.RabbitMQ;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MinGo.Messaging.Integration.Tests;

/// <summary>
/// Tests for the <c>UseRabbitMQ</c> extension method that explicitly registers
/// the RabbitMQ integration without assembly scanning.
/// </summary>
public class UseRabbitMQTests
{
    #region Service Registration

    [Fact]
    public void UseRabbitMQ_RegistersKeyedTransport()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        // Verify the keyed transport descriptor exists
        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "RabbitMQ"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseRabbitMQ_RegistersKeyedPublisher()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagePublisher) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "RabbitMQ"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseRabbitMQ_RegistersOptions()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(RabbitMQIntegrationOptions) &&
            d.Lifetime == ServiceLifetime.Singleton);

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseRabbitMQ_RegistersNonKeyedTransport_ForHostedService()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        // Non-keyed IMessagingTransport registration (for hosted service lifecycle)
        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            !d.IsKeyedService);

        Assert.NotNull(descriptor);
    }

    #endregion

    #region Configuration Binding

    [Fact]
    public void UseRabbitMQ_BindsConfigurationToOptions()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Integrations:RabbitMQ:ConnectionString"] = "amqp://user:pass@remote:5672",
            ["Messaging:Integrations:RabbitMQ:Exchange"] = "my-exchange",
            ["Messaging:Integrations:RabbitMQ:ExchangeType"] = "direct",
            ["Messaging:Integrations:RabbitMQ:Durable"] = "false",
            ["Messaging:Integrations:RabbitMQ:PrefetchCount"] = "50",
            ["Messaging:Integrations:RabbitMQ:AutomaticRecoveryEnabled"] = "false",
            ["Messaging:Integrations:RabbitMQ:MaxConnectionRetries"] = "10",
        };

        var (services, config) = Setup(configData);

        services.AddMessaging(config).UseRabbitMQ();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<RabbitMQIntegrationOptions>();

        Assert.Equal("amqp://user:pass@remote:5672", options.ConnectionString);
        Assert.Equal("my-exchange", options.Exchange);
        Assert.Equal("direct", options.ExchangeType);
        Assert.False(options.Durable);
        Assert.Equal(50, options.PrefetchCount);
        Assert.False(options.AutomaticRecoveryEnabled);
        Assert.Equal(10, options.MaxConnectionRetries);
    }

    [Fact]
    public void UseRabbitMQ_UsesDefaultOptions_WhenNoConfigSection()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<RabbitMQIntegrationOptions>();

        Assert.Equal("amqp://guest:guest@localhost:5672", options.ConnectionString);
        Assert.Equal("amq.topic", options.Exchange);
        Assert.Equal("topic", options.ExchangeType);
        Assert.True(options.Durable);
        Assert.Equal(10, options.PrefetchCount);
        Assert.True(options.AutomaticRecoveryEnabled);
        Assert.Equal(5, options.MaxConnectionRetries);
        Assert.Equal(TimeSpan.FromSeconds(5), options.RecoveryInterval);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ConnectionRetryDelay);
    }

    [Fact]
    public void UseRabbitMQ_OptionsIntegrationName_IsRabbitMQ()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseRabbitMQ();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<RabbitMQIntegrationOptions>();

        Assert.Equal("RabbitMQ", options.IntegrationName);
    }

    #endregion

    #region Builder Chaining

    [Fact]
    public void UseRabbitMQ_ReturnsSameBuilder_ForChaining()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        var result = builder.UseRabbitMQ();

        Assert.Same(builder, result);
    }

    [Fact]
    public void UseRabbitMQ_NullBuilder_ThrowsArgumentNullException()
    {
        IMessagingBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.UseRabbitMQ());
    }

    #endregion

    #region Discovery & Default Publisher

    [Fact]
    public void UseRabbitMQ_ClaimsDiscovery_PreventsImplicitScan()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        // UseRabbitMQ claims discovery, so the implicit whole-graph scan should not run
        builder.UseRabbitMQ();

        // The integration name "RabbitMQ" should be registered exactly once
        // (no duplicates from implicit scan)
        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    [Fact]
    public async Task UseRabbitMQ_SingleIntegration_EnablesDefaultPublisher()
    {
        var (services, config) = Setup();
        services.AddLogging();

        services.AddMessaging(config).UseRabbitMQ();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
    }

    [Fact]
    public void UseRabbitMQ_CalledTwice_IsIdempotent()
    {
        var (services, config) = Setup();

        services.AddMessaging(config)
            .UseRabbitMQ()
            .UseRabbitMQ();

        // Should still have exactly one keyed transport (not duplicated)
        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    #endregion

    #region Typed Publisher Integration

    [Fact]
    public async Task UseRabbitMQ_TypedPublisher_ResolvesThroughRabbitMQIntegration()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "RabbitMQ"
        };

        var (services, config) = Setup(configData);
        services.AddLogging();

        services.AddMessaging(config)
            .UseRabbitMQ()
            .AddPublishers();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<ITestOrderBusPublisher>();

        Assert.NotNull(publisher);
        Assert.IsAssignableFrom<IMessagePublisher>(publisher);
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
