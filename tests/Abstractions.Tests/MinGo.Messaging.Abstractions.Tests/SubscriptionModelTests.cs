using MinGo.Messaging;
using MinGo.Messaging.Subscriptions;
using Xunit;

namespace MinGo.Messaging.Tests;

public class SubscriptionModelTests
{
    [Fact]
    public void DeliveryMode_HasExpectedValues()
    {
        Assert.Equal(0, (int)DeliveryMode.Competing);
        Assert.Equal(1, (int)DeliveryMode.Broadcast);
    }

    [Fact]
    public void DeliveryTarget_DefaultTypeIsConsumerGroup()
    {
        var target = new DeliveryTarget
        {
            Type = DeliveryTargetType.ConsumerGroup,
            ConsumerGroupId = "my-group"
        };

        Assert.Equal(DeliveryTargetType.ConsumerGroup, target.Type);
        Assert.Equal("my-group", target.ConsumerGroupId);
        Assert.Null(target.ConsumerInstanceId);
        Assert.Null(target.Selector);
    }

    [Fact]
    public void DeliveryTarget_InstanceType_SetsInstanceId()
    {
        var target = new DeliveryTarget
        {
            Type = DeliveryTargetType.Instance,
            ConsumerInstanceId = "instance-42"
        };

        Assert.Equal(DeliveryTargetType.Instance, target.Type);
        Assert.Equal("instance-42", target.ConsumerInstanceId);
    }

    [Fact]
    public void DeliveryTarget_SelectorType_SetsSelector()
    {
        var selector = new InstanceSelector
        {
            ServiceId = "order-service",
            Labels = new Dictionary<string, string> { ["region"] = "us-east" },
            InstanceIds = new[] { "api-1", "api-2" }
        };

        var target = new DeliveryTarget
        {
            Type = DeliveryTargetType.InstanceSelector,
            Selector = selector
        };

        Assert.Equal(DeliveryTargetType.InstanceSelector, target.Type);
        Assert.NotNull(target.Selector);
        Assert.Equal("order-service", target.Selector.ServiceId);
        Assert.Contains("region", target.Selector.Labels!.Keys);
        Assert.Equal(2, target.Selector.InstanceIds!.Count);
    }

    [Fact]
    public void MessageSubscription_HasCorrectDefaults()
    {
        var subscription = new MessageSubscription
        {
            Id = "sub-001",
            ContractId = "sales.order.created",
            ContractVersion = "1",
            ConsumerServiceId = "billing-service"
        };

        Assert.Equal(DeliveryMode.Competing, subscription.DeliveryMode);
        Assert.Equal(DeliveryTargetType.ConsumerGroup, subscription.Target.Type);
    }

    [Fact]
    public void MessageSubscription_BroadcastMode_SetsCorrectly()
    {
        var subscription = new MessageSubscription
        {
            Id = "sub-002",
            ContractId = "config.changed",
            ContractVersion = "1",
            ConsumerServiceId = "cache-service",
            DeliveryMode = DeliveryMode.Broadcast,
            Target = new DeliveryTarget
            {
                Type = DeliveryTargetType.InstanceSelector,
                Selector = new InstanceSelector
                {
                    ServiceId = "cache-service",
                    Labels = new Dictionary<string, string> { ["role"] = "api" }
                }
            }
        };

        Assert.Equal(DeliveryMode.Broadcast, subscription.DeliveryMode);
        Assert.Equal(DeliveryTargetType.InstanceSelector, subscription.Target.Type);
    }
}
