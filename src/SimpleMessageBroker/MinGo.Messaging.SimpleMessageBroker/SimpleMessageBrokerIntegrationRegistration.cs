using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// Configuration entry point for the SimpleMessageBroker integration.
/// The Base SDK calls this via convention when it discovers the "SimpleMessageBroker" integration.
/// </summary>
public static class SimpleMessageBrokerIntegrationRegistration
{
    /// <summary>
    /// Configures the SimpleMessageBroker integration by binding configuration to options
    /// and registering a named HttpClient for server communication.
    /// </summary>
    /// <param name="services">The service collection to register services in.</param>
    /// <param name="section">The configuration section for this integration.</param>
    public static void Configure(IServiceCollection services, IConfiguration section)
    {
        var options = new SimpleMessageBrokerIntegrationOptions
        {
            IntegrationName = "SimpleMessageBroker"
        };

        section.Bind(options);

        // Register the options as a singleton for the transport to consume
        services.AddSingleton(options);

        // Register a named HttpClient configured for the SimpleMessageBroker server
        services.AddHttpClient("SimpleMessageBroker", client =>
        {
            client.BaseAddress = new Uri(options.BaseAddress);
            client.Timeout = TimeSpan.FromSeconds(options.ConsumeTimeoutSeconds + 10);

            if (!string.IsNullOrEmpty(options.ApiKey))
            {
                client.DefaultRequestHeaders.Add("X-Api-Key", options.ApiKey);
            }
        }).ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
        {
            MaxConnectionsPerServer = options.MaxConnectionsPerServer,
            EnableMultipleHttp2Connections = true,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        });
    }
}
