using MinGo.Messaging;
using MinGo.Messaging.Subscriptions;
using Xunit;

namespace MinGo.Messaging.Tests;

public class ConsumeContextTests
{
    [Fact]
    public void ConsumeContext_CanBeConstructed()
    {
        var message = new TestEvent("hello");
        var context = new ConsumeContext<TestEvent>
        {
            Message = message,
            MessageId = "msg-001",
            CorrelationId = "corr-001",
            SubscriptionId = "sub-001",
            ConsumerGroupId = "billing-group",
            DeliveryMode = DeliveryMode.Competing,
            Timestamp = DateTimeOffset.UtcNow,
            AttemptCount = 1,
            Headers = new Dictionary<string, object> { ["x-source"] = "test" }
        };

        Assert.Equal("hello", context.Message.Value);
        Assert.Equal("msg-001", context.MessageId);
        Assert.Equal("corr-001", context.CorrelationId);
        Assert.Equal("sub-001", context.SubscriptionId);
        Assert.Equal("billing-group", context.ConsumerGroupId);
        Assert.Null(context.ConsumerInstanceId);
        Assert.Equal(DeliveryMode.Competing, context.DeliveryMode);
        Assert.Equal(1, context.AttemptCount);
        Assert.Single(context.Headers);
    }

    [Fact]
    public void ConsumeContext_DefaultAttemptCount_IsOne()
    {
        var context = new ConsumeContext<TestEvent>
        {
            Message = new TestEvent("test"),
            MessageId = "msg-002",
            SubscriptionId = "sub-002",
            ConsumerGroupId = "group-002",
            DeliveryMode = DeliveryMode.Competing,
            Timestamp = DateTimeOffset.UtcNow
        };

        Assert.Equal(1, context.AttemptCount);
    }

    [Fact]
    public void ConsumeContext_DefaultHeaders_IsEmptyDictionary()
    {
        var context = new ConsumeContext<TestEvent>
        {
            Message = new TestEvent("test"),
            MessageId = "msg-003",
            SubscriptionId = "sub-003",
            ConsumerGroupId = "group-003",
            DeliveryMode = DeliveryMode.Competing,
            Timestamp = DateTimeOffset.UtcNow
        };

        Assert.NotNull(context.Headers);
        Assert.Empty(context.Headers);
    }

    [MessageContract(Id = "test.event", Version = "1", Kind = MessageKind.Event)]
    private sealed record TestEvent(string Value) : IEvent;
}
