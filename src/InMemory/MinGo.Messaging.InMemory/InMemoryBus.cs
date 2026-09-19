using System.Collections.Concurrent;
using System.Threading.Channels;
using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging.InMemory;

/// <summary>
/// Process-wide, thread-safe routing table backing <see cref="InMemoryMessagingTransport"/>.
/// Registered as a singleton by <see cref="InMemoryIntegrationRegistration.Configure"/> so every
/// transport instance resolved from the same <see cref="IServiceProvider"/> observes the same
/// virtual broker.
/// </summary>
/// <remarks>
/// Delivery semantics:
/// <list type="bullet">
/// <item><description>
/// <see cref="DeliveryMode.Competing"/> — one shared unbounded channel per <c>(topic, group)</c>.
/// Multiple subscribers in the same group drain the same channel, so each message reaches exactly
/// one of them, matching the "competing consumers" contract.
/// </description></item>
/// <item><description>
/// <see cref="DeliveryMode.Broadcast"/> — one channel per subscription instance. Publishing fans
/// the envelope out to every registered channel on the topic so all subscribers receive it.
/// </description></item>
/// </list>
/// Topics are matched by exact string equality; no wildcard routing is supported.
/// </remarks>
internal sealed class InMemoryBus
{
    internal readonly record struct Envelope(
        ReadOnlyMemory<byte> Payload,
        IReadOnlyDictionary<string, object> Headers);

    private sealed class TopicState
    {
        public ConcurrentDictionary<string, Channel<Envelope>> Queues { get; } = new(StringComparer.Ordinal);
    }

    private readonly ConcurrentDictionary<string, TopicState> _topics = new(StringComparer.Ordinal);

    /// <summary>
    /// Publishes an envelope to every delivery target currently registered on <paramref name="topic"/>.
    /// If no subscriber has registered the topic yet, the message is dropped (mirrors fire-and-forget
    /// broker semantics — consumers must be subscribed before publishers emit).
    /// </summary>
    public ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> data,
        IDictionary<string, object> headers,
        CancellationToken cancellationToken)
    {
        if (!_topics.TryGetValue(topic, out var state))
        {
            return ValueTask.CompletedTask;
        }

        // Snapshot header dictionary so downstream mutations by the caller cannot corrupt delivery.
        var envelope = new Envelope(
            data,
            headers.Count == 0
                ? (IReadOnlyDictionary<string, object>)EmptyHeaders.Instance
                : new Dictionary<string, object>(headers));

        var queues = state.Queues.Values.ToArray();
        if (queues.Length == 0)
        {
            return ValueTask.CompletedTask;
        }

        if (queues.Length == 1)
        {
            return queues[0].Writer.WriteAsync(envelope, cancellationToken);
        }

        return FanOutAsync(queues, envelope, cancellationToken);
    }

    /// <summary>
    /// Registers a subscription and returns the reader end of its channel.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="mode">The delivery semantics.</param>
    /// <param name="subscriptionId">The logical subscription identifier (used for Broadcast routing keys).</param>
    /// <param name="instanceId">A per-call unique identifier (used for Broadcast routing keys).</param>
    public ChannelReader<Envelope> Subscribe(
        string topic,
        string group,
        DeliveryMode mode,
        string subscriptionId,
        string instanceId)
    {
        var state = _topics.GetOrAdd(topic, static _ => new TopicState());

        // Competing: single shared queue per group — multiple subscribers drain the same channel.
        // Broadcast: one queue per (group, subscriptionId, instanceId) — every subscriber gets a copy.
        var key = mode == DeliveryMode.Broadcast
            ? $"b::{group}::{subscriptionId}::{instanceId}"
            : $"c::{group}";

        var channel = state.Queues.GetOrAdd(
            key,
            static _ => Channel.CreateUnbounded<Envelope>(
                new UnboundedChannelOptions { SingleReader = false, SingleWriter = false }));

        return channel.Reader;
    }

    private static async ValueTask FanOutAsync(
        Channel<Envelope>[] queues,
        Envelope envelope,
        CancellationToken cancellationToken)
    {
        foreach (var queue in queues)
        {
            await queue.Writer.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class EmptyHeaders : IReadOnlyDictionary<string, object>
    {
        public static readonly EmptyHeaders Instance = new();

        public object this[string key] => throw new KeyNotFoundException();
        public IEnumerable<string> Keys => Array.Empty<string>();
        public IEnumerable<object> Values => Array.Empty<object>();
        public int Count => 0;
        public bool ContainsKey(string key) => false;
        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
            => System.Linq.Enumerable.Empty<KeyValuePair<string, object>>().GetEnumerator();
        public bool TryGetValue(string key, out object value)
        {
            value = default!;
            return false;
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
