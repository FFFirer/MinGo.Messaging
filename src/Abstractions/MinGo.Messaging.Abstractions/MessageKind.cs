namespace MinGo.Messaging;

/// <summary>
/// Defines the kind of a message contract.
/// </summary>
public enum MessageKind
{
    /// <summary>
    /// An event that has occurred. Events are delivered to all subscribers (fan-out).
    /// </summary>
    Event,

    /// <summary>
    /// A command representing an intent to perform an action.
    /// </summary>
    Command
}
