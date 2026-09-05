using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.RabbitMQ;

/// <summary>
/// Configuration entry point for the RabbitMQ integration.
/// The Base SDK calls this via convention when it discovers the "RabbitMQ" integration.
/// </summary>
public static class RabbitMQIntegrationRegistration
{
    /// <summary>
    /// The integration name — used as the keyed-service key, the configuration section name
    /// (<c>Messaging:Integrations:RabbitMQ</c>), and the value referenced by
    /// <c>Messaging:Publishers:{Bus}:Integration</c>.
    /// </summary>
    public const string IntegrationName = "RabbitMQ";

    /// <summary>
    /// Configures the RabbitMQ integration by binding configuration to options.
    /// </summary>
    /// <param name="services">The service collection to register options in.</param>
    /// <param name="section">The configuration section for this integration.</param>
    public static void Configure(IServiceCollection services, IConfiguration section)
    {
        var options = new RabbitMQIntegrationOptions
        {
            IntegrationName = IntegrationName
        };

        section.Bind(options);

        // Register the options as a singleton for the transport to consume
        services.AddSingleton(options);
    }
}
