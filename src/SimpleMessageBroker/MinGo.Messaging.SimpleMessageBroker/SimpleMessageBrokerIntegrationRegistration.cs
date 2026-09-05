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
    /// The integration name — used as the keyed-service key, the configuration section name
    /// (<c>Messaging:Integrations:SimpleMessageBroker</c>), and the value referenced by
    /// <c>Messaging:Publishers:{Bus}:Integration</c>.
    /// </summary>
    public const string IntegrationName = "SimpleMessageBroker";

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
            IntegrationName = IntegrationName
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
