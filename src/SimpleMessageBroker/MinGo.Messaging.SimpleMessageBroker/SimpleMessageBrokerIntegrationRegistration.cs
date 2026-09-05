using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SimpleMessageBroker.Client.Extensions;

namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// Configuration entry point for the SimpleMessageBroker integration.
/// The Base SDK calls this via convention when it discovers the "SimpleMessageBroker" integration.
/// </summary>
public static class SimpleMessageBrokerIntegrationRegistration
{
    /// <summary>
    /// Configures the SimpleMessageBroker integration by binding configuration to options
    /// and registering the SimpleMessageBroker Client SDK services.
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

        // Register the SimpleMessageBroker Client SDK (HttpClient, options, IMessageQueueClient)
        services.AddMessageQueueClient(smbOptions =>
        {
            smbOptions.BaseAddress = options.BaseAddress;
            smbOptions.ApiKey = options.ApiKey;
            smbOptions.MaxConnectionsPerServer = options.MaxConnectionsPerServer;
        });
    }
}
