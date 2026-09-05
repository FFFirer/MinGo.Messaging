using System.Text.Json;
using MinGo.Messaging.Serialization;

namespace MinGo.Messaging.Serialization;

/// <summary>
/// Default message serializer based on System.Text.Json.
/// </summary>
internal sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    public ReadOnlyMemory<byte> Serialize(IMessage message, Type messageType)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, messageType, DefaultOptions);
    }

    public IMessage? Deserialize(ReadOnlyMemory<byte> data, Type messageType)
    {
        return (IMessage?)JsonSerializer.Deserialize(data.Span, messageType, DefaultOptions);
    }
}
