using MinGo.Messaging;
using Xunit;

namespace MinGo.Messaging.Tests;

public class MessageContractAttributeTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        var attr = new MessageContractAttribute();

        Assert.Equal(string.Empty, attr.Id);
        Assert.Equal("1", attr.Version);
        Assert.Equal(MessageKind.Event, attr.Kind);
    }

    [Fact]
    public void CanSetAllProperties()
    {
        var attr = new MessageContractAttribute
        {
            Id = "sales.order.created",
            Version = "2",
            Kind = MessageKind.Command
        };

        Assert.Equal("sales.order.created", attr.Id);
        Assert.Equal("2", attr.Version);
        Assert.Equal(MessageKind.Command, attr.Kind);
    }

    [Fact]
    public void Attribute_CanBeAppliedToClass()
    {
        var attr = typeof(TestMessage).GetCustomAttributes(typeof(MessageContractAttribute), false);

        Assert.Single(attr);

        var contract = (MessageContractAttribute)attr[0];
        Assert.Equal("test.message", contract.Id);
        Assert.Equal("1", contract.Version);
        Assert.Equal(MessageKind.Event, contract.Kind);
    }

    [MessageContract(Id = "test.message", Version = "1", Kind = MessageKind.Event)]
    private sealed record TestMessage(string Value) : IEvent;
}
