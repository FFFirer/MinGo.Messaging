using System.Reflection;
using MinGo.Messaging.Internal;
using Moq;
using Xunit;

namespace MinGo.Messaging.Tests;

/// <summary>
/// Tests for <see cref="TypedMessagePublisher"/> — the DispatchProxy-based dynamic proxy
/// that implements typed message bus publisher interfaces (e.g. IOrderBusPublisher).
/// </summary>
public class TypedMessagePublisherTests
{
    #region Proxy Creation

    [Fact]
    public void Create_ProducesProxyThatImplementsTypedInterface()
    {
        var innerMock = new Mock<IMessagePublisher>();
        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);

        Assert.IsAssignableFrom<ITestBusPublisher>(proxy);
        Assert.IsAssignableFrom<IMessagePublisher>(proxy);
    }

    [Fact]
    public void Initialize_WithNull_ThrowsArgumentNullException()
    {
        var proxy = new TypedMessagePublisher();
        Assert.Throws<ArgumentNullException>(() => proxy.Initialize(null!));
    }

    #endregion

    #region Method Delegation

    [Fact]
    public async Task PublishAsync_DelegatesToInnerPublisher()
    {
        var innerMock = new Mock<IMessagePublisher>();
        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("hello");

        await proxy.PublishAsync(message);

        innerMock.Verify(
            p => p.PublishAsync(message, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_DelegatesToInnerPublisher()
    {
        var innerMock = new Mock<IMessagePublisher>();
        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("command");

        await proxy.SendAsync(message);

        innerMock.Verify(
            p => p.SendAsync(message, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_PassesCancellationToken_Through()
    {
        using var cts = new CancellationTokenSource();
        var innerMock = new Mock<IMessagePublisher>();
        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("test");

        await proxy.PublishAsync(message, cts.Token);

        innerMock.Verify(
            p => p.PublishAsync(message, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_PassesCancellationToken_Through()
    {
        using var cts = new CancellationTokenSource();
        var innerMock = new Mock<IMessagePublisher>();
        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("test");

        await proxy.SendAsync(message, cts.Token);

        innerMock.Verify(
            p => p.SendAsync(message, cts.Token),
            Times.Once);
    }

    #endregion

    #region Exception Propagation

    [Fact]
    public async Task PublishAsync_PropagatesInnerException()
    {
        var innerMock = new Mock<IMessagePublisher>();
        innerMock
            .Setup(p => p.PublishAsync(It.IsAny<IMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("publish failed"));

        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("test");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => proxy.PublishAsync(message));
        Assert.Equal("publish failed", ex.Message);
    }

    [Fact]
    public async Task SendAsync_PropagatesInnerException()
    {
        var innerMock = new Mock<IMessagePublisher>();
        innerMock
            .Setup(p => p.SendAsync(It.IsAny<IMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("send failed"));

        var proxy = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var message = new TestMessage("test");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => proxy.SendAsync(message));
        Assert.Equal("send failed", ex.Message);
    }

    #endregion

    #region Multiple Typed Interfaces

    [Fact]
    public async Task DifferentTypedInterfaces_DelegateToSameInnerPublisher()
    {
        var innerMock = new Mock<IMessagePublisher>();
        var message = new TestMessage("shared");

        var proxy1 = CreateProxy<ITestBusPublisher>(innerMock.Object);
        var proxy2 = CreateProxy<IAnotherBusPublisher>(innerMock.Object);

        await proxy1.PublishAsync(message);
        await proxy2.PublishAsync(message);

        innerMock.Verify(
            p => p.PublishAsync(message, It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    #endregion

    #region Helpers

    private static T CreateProxy<T>(IMessagePublisher inner) where T : IMessagePublisher
    {
        var proxy = (TypedMessagePublisher)DispatchProxy.Create(typeof(T), typeof(TypedMessagePublisher));
        proxy.Initialize(inner);
        return (T)(object)proxy;
    }

    #endregion

    #region Test Types

    [MessageBus("TestBus")]
    private interface ITestBusPublisher : IMessagePublisher { }

    [MessageBus("AnotherBus")]
    private interface IAnotherBusPublisher : IMessagePublisher { }

    [MessageContract(Id = "test.message", Version = "1", Kind = MessageKind.Event)]
    private sealed record TestMessage(string Value) : IEvent;

    #endregion
}
