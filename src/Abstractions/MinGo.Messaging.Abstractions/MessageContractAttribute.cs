namespace MinGo.Messaging;

/// <summary>
/// Defines a message contract with a unique identifier, version, and kind.
/// Applied to message classes to declare their identity in the messaging system.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MessageContractAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the unique identifier for this message contract (e.g. "sales.order.created").
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the version of this contract. Defaults to "1".
    /// </summary>
    public string Version { get; set; } = "1";

    /// <summary>
    /// Gets or sets the kind of message (Event or Command).
    /// </summary>
    public MessageKind Kind { get; set; }
}
