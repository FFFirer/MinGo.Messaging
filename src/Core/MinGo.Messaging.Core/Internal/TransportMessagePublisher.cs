using MinGo.Messaging.Serialization;
using MinGo.Messaging.Transport;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Message publisher that delegates to a specific IMessagingTransport.
/// </summary>
internal sealed class TransportMessagePublisher : IMessagePublisher
{
    private readonly IMessagingTransport _transport;
    private readonly IMessageSerializer _serializer;

    public TransportMessagePublisher(IMessagingTransport transport, IMessageSerializer serializer)
    {
        _transport = transport;
        _serializer = serializer;
    }

    public async Task PublishAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        var messageType = message.GetType();
        var contractAttr = System.Reflection.CustomAttributeExtensions
            .GetCustomAttribute<MessageContractAttribute>(messageType);

        var topic = contractAttr?.Id ?? messageType.FullName ?? messageType.Name;
        var data = _serializer.Serialize(message, messageType);
        var headers = new Dictionary<string, object>
        {
            ["x-message-id"] = Guid.NewGuid().ToString("N"),
            ["x-message-type"] = messageType.FullName ?? messageType.Name,
            ["x-timestamp"] = DateTimeOffset.UtcNow.ToString("O")
        };

        if (contractAttr is not null)
        {
            headers["x-contract-version"] = contractAttr.Version;
        }

        await _transport.PublishAsync(topic, data, headers, cancellationToken);
    }

    public Task SendAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        // For commands, same transport but routing may differ at integration level
        return PublishAsync(message, cancellationToken);
    }
}
