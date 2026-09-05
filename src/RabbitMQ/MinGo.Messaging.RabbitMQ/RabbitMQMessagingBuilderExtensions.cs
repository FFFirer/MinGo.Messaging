namespace MinGo.Messaging.RabbitMQ;

/// <summary>
/// Explicit registration extensions for the RabbitMQ integration.
/// </summary>
/// <remarks>
/// <see cref="UseRabbitMQ"/> registers the RabbitMQ transport and its keyed
/// <see cref="IMessagePublisher"/> directly — no assembly scanning is involved. It is the
/// AOT/trimming-friendly alternative to <c>AddIntegrations(...)</c>, and it claims integration
/// discovery, so the implicit whole-dependency-graph scan will not also run. Call it before
/// <c>AddPublishers</c>/<c>AddConsumer</c> so their transports are available.
/// </remarks>
public static class RabbitMQMessagingBuilderExtensions
{
    /// <summary>
    /// Registers the RabbitMQ integration explicitly, binding <see cref="RabbitMQIntegrationOptions"/>
    /// from the <c>Messaging:Integrations:RabbitMQ</c> configuration section. When the section is
    /// absent, the option defaults are used.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IMessagingBuilder UseRabbitMQ(this IMessagingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Bind options + register any RabbitMQ-specific services (reuses the same Configure hook
        // the assembly-scanning path invokes, so both paths stay behaviourally identical).
        RabbitMQIntegrationRegistration.Configure(
            builder.Services,
            builder.Configuration.GetSection($"Messaging:Integrations:{RabbitMQIntegrationRegistration.IntegrationName}"));

        // Register the keyed transport + publisher with no reflection over the dependency graph.
        return builder.AddIntegration<RabbitMQMessagingTransport>(RabbitMQIntegrationRegistration.IntegrationName);
    }
}
