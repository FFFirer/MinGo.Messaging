using MinGo.Messaging.Integration;

namespace MinGo.Messaging.InMemory;

/// <summary>
/// Configuration options for the In-Memory integration.
/// </summary>
/// <remarks>
/// The In-Memory transport runs entirely inside the current process and has no external
/// endpoints to bind. The options type is retained for symmetry with the other integrations
/// and to allow future extension (e.g. bounded channel capacity, simulated latency) without
/// breaking the <c>Messaging:Integrations:InMemory</c> configuration section contract.
/// </remarks>
public sealed class InMemoryIntegrationOptions : MessagingIntegrationOptions
{
}
