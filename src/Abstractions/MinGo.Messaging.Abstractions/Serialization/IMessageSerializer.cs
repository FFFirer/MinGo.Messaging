namespace MinGo.Messaging.Serialization;

/// <summary>
/// Serializes and deserializes messages for transport.
/// Integration SDKs use this SPI to convert messages to/from byte representations.
/// Can be replaced with a custom implementation via <c>IMessagingBuilder.ConfigureSerializer</c>.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message to bytes.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="messageType">The concrete type of the message.</param>
    /// <returns>The serialized byte representation.</returns>
    ReadOnlyMemory<byte> Serialize(IMessage message, Type messageType);

    /// <summary>
    /// Deserializes bytes back to a message.
    /// </summary>
    /// <param name="data">The serialized message bytes.</param>
    /// <param name="messageType">The expected message type.</param>
    /// <returns>The deserialized message, or null if deserialization fails.</returns>
    IMessage? Deserialize(ReadOnlyMemory<byte> data, Type messageType);
}
