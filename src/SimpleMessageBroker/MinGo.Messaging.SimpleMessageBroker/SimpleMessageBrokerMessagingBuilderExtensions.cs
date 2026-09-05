namespace MinGo.Messaging.SimpleMessageBroker;

/// <summary>
/// Explicit registration extensions for the SimpleMessageBroker integration.
/// </summary>
/// <remarks>
/// <see cref="UseSimpleMessageBroker"/> registers the SimpleMessageBroker transport, its keyed
/// <see cref="IMessagePublisher"/>, and the underlying Client SDK services directly — no assembly
/// scanning is involved. It is the AOT/trimming-friendly alternative to <c>AddIntegrations(...)</c>,
/// and it claims integration discovery, so the implicit whole-dependency-graph scan will not also
/// run. Call it before <c>AddPublishers</c>/<c>AddConsumer</c> so their transports are available.
/// </remarks>
public static class SimpleMessageBrokerMessagingBuilderExtensions
{
    /// <summary>
    /// Registers the SimpleMessageBroker integration explicitly, binding
    /// <see cref="SimpleMessageBrokerIntegrationOptions"/> from the
    /// <c>Messaging:Integrations:SimpleMessageBroker</c> configuration section. When the section is
    /// absent, the option defaults are used.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IMessagingBuilder UseSimpleMessageBroker(this IMessagingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Bind options + register the SimpleMessageBroker Client SDK (reuses the same Configure hook
        // the assembly-scanning path invokes, so both paths stay behaviourally identical).
        SimpleMessageBrokerIntegrationRegistration.Configure(
            builder.Services,
            builder.Configuration.GetSection($"Messaging:Integrations:{SimpleMessageBrokerIntegrationRegistration.IntegrationName}"));

        // Register the keyed transport + publisher with no reflection over the dependency graph.
        return builder.AddIntegration<SimpleMessageBrokerMessagingTransport>(SimpleMessageBrokerIntegrationRegistration.IntegrationName);
    }
}
