namespace MinGo.Messaging;

/// <summary>
/// Base marker interface for all messages in the messaging system.
/// </summary>
public interface IMessage;

/// <summary>
/// Marker interface for event messages.
/// Events represent something that has happened and are delivered to multiple subscribers (fan-out).
/// </summary>
public interface IEvent : IMessage;

/// <summary>
/// Marker interface for command messages.
/// Commands represent an intent to perform an action and are typically delivered to a single consumer.
/// </summary>
public interface ICommand : IMessage;
