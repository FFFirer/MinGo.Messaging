using System.Globalization;
using System.Reflection;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
using MinGo.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Bridges the <see cref="IMessagingTransport"/> SPI with <see cref="IConsumer{TMessage}"/> instances.
/// Creates a per-subscription handler callback that deserialises incoming bytes, resolves the
/// consumer from DI, and invokes <see cref="IConsumer{TMessage}.ConsumeAsync"/>.
/// </summary>
internal sealed class ConsumerDispatcher
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<ConsumerDispatcher> _logger;

    public ConsumerDispatcher(
        IServiceScopeFactory scopeFactory,
        IMessageSerializer serializer,
        ILogger<ConsumerDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _serializer = serializer;
        _logger = logger;
    }

    /// <summary>
    /// Builds the handler callback that <see cref="IMessagingTransport.SubscribeAsync"/> expects
    /// for the given <paramref name="subscription"/>.
    /// </summary>
    /// <remarks>
    /// The returned delegate is called once per message delivery. It:
    /// <list type="number">
    ///   <item>Deserialises the payload to <see cref="MessageSubscription.MessageType"/>.</item>
    ///   <item>Creates a DI scope and resolves <see cref="MessageSubscription.ConsumerType"/>.</item>
    ///   <item>Constructs a <c>ConsumeContext&lt;TMessage&gt;</c> via reflection.</item>
    ///   <item>Invokes <c>IConsumer&lt;TMessage&gt;.ConsumeAsync</c> and maps the outcome to
    ///     <see cref="ConsumeResult"/>.</item>
    /// </list>
    /// </remarks>
    public Func<ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken, Task<ConsumeResult>>
        CreateHandler(MessageSubscription subscription)
    {
        if (subscription.MessageType is null)
            throw new InvalidOperationException(
                $"Subscription '{subscription.Id}' has no MessageType set. " +
                "Ensure SubscriptionBuilder populated MessageType from the consumer attribute.");

        if (subscription.ConsumerType is null)
            throw new InvalidOperationException(
                $"Subscription '{subscription.Id}' has no ConsumerType set. " +
                "Ensure SubscriptionBuilder populated ConsumerType from the consumer attribute.");

        var messageType = subscription.MessageType;
        var consumerType = subscription.ConsumerType;

        // Resolve ConsumeContext<TMessage> once per subscription (generic type is fixed).
        var contextType = typeof(ConsumeContext<>).MakeGenericType(messageType);

        // Locate ConsumeAsync on the closed IConsumer<TMessage> interface implemented by ConsumerType.
        var consumerInterface = typeof(IConsumer<>).MakeGenericType(messageType);
        var consumeMethod = consumerInterface.GetMethod(
            nameof(IConsumer<IMessage>.ConsumeAsync),
            BindingFlags.Public | BindingFlags.Instance);

        if (consumeMethod is null)
            throw new InvalidOperationException(
                $"Could not find ConsumeAsync on {consumerInterface.FullName}.");

        return async (data, headers, cancellationToken) =>
        {
            // ── 1. Deserialise ──────────────────────────────────────────────────────────────────
            IMessage? message;
            try
            {
                message = _serializer.Deserialize(data, messageType);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Deserialization failed for subscription {SubscriptionId} (message type {MessageType}).",
                    subscription.Id, messageType.Name);
                return ConsumeResult.Nack;
            }

            if (message is null)
            {
                _logger.LogWarning(
                    "Deserialization returned null for subscription {SubscriptionId}; nacking message.",
                    subscription.Id);
                return ConsumeResult.Nack;
            }

            // ── 2. Extract metadata from headers ─────────────────────────────────────────────────
            var messageId = headers.TryGetValue("x-message-id", out var mid)
                ? mid?.ToString() ?? Guid.NewGuid().ToString("N")
                : Guid.NewGuid().ToString("N");

            var timestamp = headers.TryGetValue("x-timestamp", out var ts)
                ? ParseTimestamp(ts)
                : DateTimeOffset.UtcNow;

            var copyHeaders = new Dictionary<string, object>(headers);

            // ── 3. Create a DI scope and resolve the consumer ───────────────────────────────────
            using var scope = _scopeFactory.CreateScope();

            object consumer;
            try
            {
                consumer = scope.ServiceProvider.GetRequiredService(consumerType);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to resolve consumer {ConsumerType} for subscription {SubscriptionId}.",
                    consumerType.Name, subscription.Id);
                return ConsumeResult.Nack;
            }

            // ── 4. Build ConsumeContext<TMessage> via reflection ────────────────────────────────
            var context = Activator.CreateInstance(contextType)!;
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.Message), message);
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.MessageId), messageId);
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.SubscriptionId), subscription.Id);
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.ConsumerGroupId),
                subscription.Target.ConsumerGroupId ?? subscription.ConsumerServiceId);
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.DeliveryMode), subscription.DeliveryMode);
            SetRequiredProperty(contextType, context, nameof(ConsumeContext<IMessage>.Timestamp), timestamp);
            SetOptionalProperty(contextType, context, nameof(ConsumeContext<IMessage>.Headers), copyHeaders);
            SetOptionalProperty(contextType, context, nameof(ConsumeContext<IMessage>.CancellationToken), cancellationToken);

            // ── 5. Invoke IConsumer<TMessage>.ConsumeAsync ──────────────────────────────────────
            try
            {
                var task = (Task)consumeMethod.Invoke(consumer, [context, cancellationToken])!;
                await task.ConfigureAwait(false);
                return ConsumeResult.Ack;
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                _logger.LogError(ex.InnerException,
                    "Consumer {ConsumerType} threw an exception while processing message {MessageId} " +
                    "(subscription {SubscriptionId}). The message will be retried.",
                    consumerType.Name, messageId, subscription.Id);
                return ConsumeResult.Retry;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Processing cancelled for message {MessageId} (subscription {SubscriptionId}); retrying.",
                    messageId, subscription.Id);
                return ConsumeResult.Retry;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Unexpected error in consumer {ConsumerType} for message {MessageId} " +
                    "(subscription {SubscriptionId}). The message will be retried.",
                    consumerType.Name, messageId, subscription.Id);
                return ConsumeResult.Retry;
            }
        };
    }

    private static void SetRequiredProperty(Type type, object instance, string name, object? value)
    {
        type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance)
            ?.SetValue(instance, value);
    }

    private static void SetOptionalProperty(Type type, object instance, string name, object? value)
    {
        type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance)
            ?.SetValue(instance, value);
    }

    private static DateTimeOffset ParseTimestamp(object? value)
    {
        if (value is string s &&
            DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var result))
        {
            return result;
        }

        return DateTimeOffset.UtcNow;
    }
}
