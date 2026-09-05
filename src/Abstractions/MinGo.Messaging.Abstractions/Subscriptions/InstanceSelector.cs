namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Selects consumer instances by service identity, labels, or explicit instance IDs.
/// Used for targeted delivery in broadcast scenarios.
/// </summary>
public sealed class InstanceSelector
{
    /// <summary>
    /// Gets the service identifier to select instances from.
    /// </summary>
    public required string ServiceId { get; init; }

    /// <summary>
    /// Gets the label key-value pairs to filter instances.
    /// </summary>
    public Dictionary<string, string>? Labels { get; init; }

    /// <summary>
    /// Gets the explicit instance IDs to target.
    /// </summary>
    public IReadOnlyCollection<string>? InstanceIds { get; init; }
}
