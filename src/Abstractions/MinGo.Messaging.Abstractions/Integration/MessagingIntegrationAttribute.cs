namespace MinGo.Messaging.Integration;

/// <summary>
/// Assembly-level attribute that allows an Integration SDK to self-discover.
/// The Base SDK scans for this attribute to find available integrations.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
public sealed class MessagingIntegrationAttribute : Attribute
{
    /// <summary>
    /// Gets the unique name of this integration (e.g. "RabbitMQ", "Kafka").
    /// This name is used as the routing key in configuration and named publishers.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the <see cref="Transport.IMessagingTransport"/> implementation type.
    /// </summary>
    public Type TransportType { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessagingIntegrationAttribute"/> class.
    /// </summary>
    /// <param name="name">The integration name.</param>
    /// <param name="transportType">The transport implementation type.</param>
    public MessagingIntegrationAttribute(string name, Type transportType)
    {
        Name = name;
        TransportType = transportType;
    }
}
