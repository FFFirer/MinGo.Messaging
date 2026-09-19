namespace MinGo.Messaging.InMemory;

/// <summary>
/// Explicit registration extensions for the In-Memory integration.
/// </summary>
/// <remarks>
/// <see cref="UseInMemory"/> registers the In-Memory transport, its keyed
/// <see cref="IMessagePublisher"/>, and the shared <c>InMemoryBus</c> singleton directly — no
/// assembly scanning is involved. It is the AOT/trimming-friendly alternative to
/// <c>AddIntegrations(...)</c>, and it claims integration discovery, so the implicit
/// whole-dependency-graph scan will not also run. Call it before <c>AddPublishers</c>/<c>AddConsumer</c>
/// so their transports are available.
/// </remarks>
public static class InMemoryMessagingBuilderExtensions
{
    /// <summary>
    /// Registers the In-Memory integration explicitly, binding
    /// <see cref="InMemoryIntegrationOptions"/> from the <c>Messaging:Integrations:InMemory</c>
    /// configuration section. When the section is absent, the option defaults are used.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IMessagingBuilder UseInMemory(this IMessagingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Bind options + register the shared bus (reuses the same Configure hook the
        // assembly-scanning path invokes, so both paths stay behaviourally identical).
        InMemoryIntegrationRegistration.Configure(
            builder.Services,
            builder.Configuration.GetSection($"Messaging:Integrations:{InMemoryIntegrationRegistration.IntegrationName}"));

        // Register the keyed transport + publisher with no reflection over the dependency graph.
        return builder.AddIntegration<InMemoryMessagingTransport>(InMemoryIntegrationRegistration.IntegrationName);
    }
}
