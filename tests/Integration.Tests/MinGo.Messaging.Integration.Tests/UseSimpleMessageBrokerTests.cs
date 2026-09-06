using MinGo.Messaging.SimpleMessageBroker;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SimpleMessageBroker.Client;
using Xunit;

namespace MinGo.Messaging.Integration.Tests;

/// <summary>
/// Tests for the <c>UseSimpleMessageBroker</c> extension method that explicitly registers
/// the SimpleMessageBroker integration without assembly scanning.
/// </summary>
public class UseSimpleMessageBrokerTests
{
    #region Service Registration

    [Fact]
    public void UseSimpleMessageBroker_RegistersKeyedTransport()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "SimpleMessageBroker"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseSimpleMessageBroker_RegistersKeyedPublisher()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagePublisher) &&
            d.IsKeyedService &&
            Equals(d.ServiceKey, "SimpleMessageBroker"));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseSimpleMessageBroker_RegistersOptions()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(SimpleMessageBrokerIntegrationOptions) &&
            d.Lifetime == ServiceLifetime.Singleton);

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseSimpleMessageBroker_RegistersClientSDK_IMessageQueueClient()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        // SimpleMessageBrokerIntegrationRegistration.Configure registers IMessageQueueClient
        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessageQueueClient));

        Assert.NotNull(descriptor);
    }

    [Fact]
    public void UseSimpleMessageBroker_RegistersNonKeyedTransport_ForHostedService()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessagingTransport) &&
            !d.IsKeyedService);

        Assert.NotNull(descriptor);
    }

    #endregion

    #region Configuration Binding

    [Fact]
    public void UseSimpleMessageBroker_BindsConfigurationToOptions()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Integrations:SimpleMessageBroker:BaseAddress"] = "http://custom-host:8080",
            ["Messaging:Integrations:SimpleMessageBroker:ApiKey"] = "secret-key-123",
            ["Messaging:Integrations:SimpleMessageBroker:PollingInterval"] = "00:00:03",
            ["Messaging:Integrations:SimpleMessageBroker:BatchSize"] = "25",
            ["Messaging:Integrations:SimpleMessageBroker:ConsumeTimeoutSeconds"] = "10",
            ["Messaging:Integrations:SimpleMessageBroker:MaxConnectionsPerServer"] = "100",
            ["Messaging:Integrations:SimpleMessageBroker:MaxConnectionRetries"] = "3",
        };

        var (services, config) = Setup(configData);

        services.AddMessaging(config).UseSimpleMessageBroker();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<SimpleMessageBrokerIntegrationOptions>();

        Assert.Equal("http://custom-host:8080", options.BaseAddress);
        Assert.Equal("secret-key-123", options.ApiKey);
        Assert.Equal(TimeSpan.FromSeconds(3), options.PollingInterval);
        Assert.Equal(25, options.BatchSize);
        Assert.Equal(10, options.ConsumeTimeoutSeconds);
        Assert.Equal(100, options.MaxConnectionsPerServer);
        Assert.Equal(3, options.MaxConnectionRetries);
    }

    [Fact]
    public void UseSimpleMessageBroker_UsesDefaultOptions_WhenNoConfigSection()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<SimpleMessageBrokerIntegrationOptions>();

        Assert.Equal("http://localhost:5000", options.BaseAddress);
        Assert.Null(options.ApiKey);
        Assert.Equal(TimeSpan.FromSeconds(1), options.PollingInterval);
        Assert.Equal(10, options.BatchSize);
        Assert.Equal(5, options.ConsumeTimeoutSeconds);
        Assert.Equal(50, options.MaxConnectionsPerServer);
        Assert.Equal(5, options.MaxConnectionRetries);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ConnectionRetryDelay);
    }

    [Fact]
    public void UseSimpleMessageBroker_OptionsIntegrationName_IsSimpleMessageBroker()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<SimpleMessageBrokerIntegrationOptions>();

        Assert.Equal("SimpleMessageBroker", options.IntegrationName);
    }

    #endregion

    #region Builder Chaining

    [Fact]
    public void UseSimpleMessageBroker_ReturnsSameBuilder_ForChaining()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        var result = builder.UseSimpleMessageBroker();

        Assert.Same(builder, result);
    }

    [Fact]
    public void UseSimpleMessageBroker_NullBuilder_ThrowsArgumentNullException()
    {
        IMessagingBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.UseSimpleMessageBroker());
    }

    #endregion

    #region Discovery & Default Publisher

    [Fact]
    public void UseSimpleMessageBroker_ClaimsDiscovery_PreventsImplicitScan()
    {
        var (services, config) = Setup();
        var builder = services.AddMessaging(config);

        builder.UseSimpleMessageBroker();

        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    [Fact]
    public async Task UseSimpleMessageBroker_SingleIntegration_EnablesDefaultPublisher()
    {
        var (services, config) = Setup();

        services.AddMessaging(config).UseSimpleMessageBroker();

        await using var sp = services.BuildServiceProvider();
        var publisher = sp.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
    }

    [Fact]
    public void UseSimpleMessageBroker_CalledTwice_IsIdempotent()
    {
        var (services, config) = Setup();

        services.AddMessaging(config)
            .UseSimpleMessageBroker()
            .UseSimpleMessageBroker();

        var transportDescriptors = services
            .Where(d => d.ServiceType == typeof(IMessagingTransport) && d.IsKeyedService)
            .ToList();

        Assert.Single(transportDescriptors);
    }

    #endregion

    #region Typed Publisher Integration

    [Fact]
    public async Task UseSimpleMessageBroker_TypedPublisher_ResolvesThroughIntegration()
    {
        var configData = new Dictionary<string, string?>
        {
            ["Messaging:Publishers:OrderBus:Integration"] = "SimpleMessageBroker"
        };

        var (services, config) = Setup(configData);

        services.AddMessaging(config)
            .UseSimpleMessageBroker()
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
