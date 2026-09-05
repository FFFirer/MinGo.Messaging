namespace MinGo.Messaging.Integration;

/// <summary>
/// Base class for integration-specific configuration options.
/// Each Integration SDK extends this with its own configuration properties.
/// </summary>
public abstract class MessagingIntegrationOptions
{
    /// <summary>
    /// Gets or sets the integration name this options instance applies to.
    /// </summary>
    public string IntegrationName { get; set; } = string.Empty;
}
