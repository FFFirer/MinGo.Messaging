using System.Reflection;

namespace MinGo.Messaging.Integration;

/// <summary>
/// Describes a discovered Integration SDK.
/// </summary>
internal sealed record IntegrationDescriptor(
    string Name,
    Type TransportType,
    Assembly Assembly);
