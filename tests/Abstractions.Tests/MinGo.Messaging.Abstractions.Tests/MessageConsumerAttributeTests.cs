using MinGo.Messaging;
using MinGo.Messaging.Subscriptions;
using Xunit;

namespace MinGo.Messaging.Tests;

public class MessageConsumerAttributeTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        var attr = new MessageConsumerAttribute();

        Assert.Equal(typeof(object), attr.Contract);
        Assert.Equal(string.Empty, attr.Group);
        Assert.Equal(DeliveryMode.Competing, attr.DeliveryMode);
    }

    [Fact]
    public void CanSetAllProperties()
    {
        var attr = new MessageConsumerAttribute
        {
            Contract = typeof(TestEvent),
            Group = "my-group",
            DeliveryMode = DeliveryMode.Broadcast
        };

        Assert.Equal(typeof(TestEvent), attr.Contract);
        Assert.Equal("my-group", attr.Group);
        Assert.Equal(DeliveryMode.Broadcast, attr.DeliveryMode);
    }

    [Fact]
    public void Attribute_AllowsMultiple()
    {
        var attrs = typeof(MultiConsumer).GetCustomAttributes(typeof(MessageConsumerAttribute), false);

        Assert.Equal(2, attrs.Length);
    }

    private sealed record TestEvent(string Value) : IEvent;

    [MessageConsumer(Contract = typeof(TestEvent), Group = "group-a")]
    [MessageConsumer(Contract = typeof(TestEvent), Group = "group-b", DeliveryMode = DeliveryMode.Broadcast)]
    private sealed class MultiConsumer { }
}
