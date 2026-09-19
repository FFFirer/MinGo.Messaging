using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.InMemory;

/// <summary>
/// Configuration entry point for the In-Memory integration.
/// The Base SDK calls this via convention when it discovers the "InMemory" integration.
/// </summary>
public static class InMemoryIntegrationRegistration
{
    /// <summary>
    /// The integration name — used as the keyed-service key, the configuration section name
    /// (<c>Messaging:Integrations:InMemory</c>), and the value referenced by
    /// <c>Messaging:Publishers:{Bus}:Integration</c>.
    /// </summary>
    public const string IntegrationName = "InMemory";

    /// <summary>
    /// Configures the In-Memory integration by binding configuration to options and registering
    /// the process-wide <see cref="InMemoryBus"/> singleton that backs every transport instance
    /// resolved from the same <see cref="IServiceProvider"/>.
    /// </summary>
    /// <param name="services">The service collection to register services in.</param>
    /// <param name="section">The configuration section for this integration.</param>
    public static void Configure(IServiceCollection services, IConfiguration section)
    {
        var options = new InMemoryIntegrationOptions
        {
            IntegrationName = IntegrationName
        };

        section.Bind(options);

        services.AddSingleton(options);

        // The bus is the shared, process-wide routing table. Every keyed transport instance
        // resolves the same singleton so publishers and consumers see one virtual broker.
        services.AddSingleton<InMemoryBus>();
    }
}
