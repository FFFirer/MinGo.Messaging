using MinGo.Messaging.Internal;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Transport;
using Moq;
using Xunit;

namespace MinGo.Messaging.Tests;

/// <summary>
/// Tests for <see cref="TransportMessagePublisher"/> — the core IMessagePublisher implementation
/// that delegates to IMessagingTransport.
/// </summary>
public class TransportMessagePublisherTests
{
    private readonly Mock<IMessagingTransport> _transportMock = new();
    private readonly Mock<IMessageSerializer> _serializerMock = new();
    private readonly TransportMessagePublisher _publisher;

    public TransportMessagePublisherTests()
    {
        _publisher = new TransportMessagePublisher(_transportMock.Object, _serializerMock.Object);
    }

    #region PublishAsync — Topic Resolution

    [Fact]
    public async Task PublishAsync_WithMessageContract_UsesContractIdAsTopic()
    {
        var message = new ContractedEvent();
        var serializedData = new byte[] { 1, 2, 3 };
        _serializerMock.Setup(s => s.Serialize(message, typeof(ContractedEvent)))
            .Returns(serializedData);

        await _publisher.PublishAsync(message);

        _transportMock.Verify(
            t => t.PublishAsync(
                "sales.order.created",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WithoutMessageContract_UsesFullTypeNameAsTopic()
    {
        var message = new UncontractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, typeof(UncontractedEvent)))
            .Returns(new byte[] { 1 });

        await _publisher.PublishAsync(message);

        var expectedTopic = typeof(UncontractedEvent).FullName ?? typeof(UncontractedEvent).Name;
        _transportMock.Verify(
            t => t.PublishAsync(
                expectedTopic,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    #endregion

    #region PublishAsync — Header Construction

    [Fact]
    public async Task PublishAsync_AlwaysIncludesStandardHeaders()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        IDictionary<string, object>? capturedHeaders = null;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => capturedHeaders = headers);

        await _publisher.PublishAsync(message);

        Assert.NotNull(capturedHeaders);
        Assert.Contains("x-message-id", capturedHeaders.Keys);
        Assert.Contains("x-message-type", capturedHeaders.Keys);
        Assert.Contains("x-timestamp", capturedHeaders.Keys);
    }

    [Fact]
    public async Task PublishAsync_MessageIdHeader_IsNonEmptyGuidString()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        IDictionary<string, object>? capturedHeaders = null;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => capturedHeaders = headers);

        await _publisher.PublishAsync(message);

        var messageId = (string)capturedHeaders!["x-message-id"];
        Assert.False(string.IsNullOrWhiteSpace(messageId));
        // "N" format produces a 32-char hex string (no hyphens)
        Assert.Equal(32, messageId.Length);
    }

    [Fact]
    public async Task PublishAsync_MessageTypeHeader_ContainsFullTypeName()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        IDictionary<string, object>? capturedHeaders = null;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => capturedHeaders = headers);

        await _publisher.PublishAsync(message);

        var messageType = (string)capturedHeaders!["x-message-type"];
        Assert.Equal(typeof(ContractedEvent).FullName, messageType);
    }

    [Fact]
    public async Task PublishAsync_WithMessageContract_IncludesContractVersionHeader()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        IDictionary<string, object>? capturedHeaders = null;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => capturedHeaders = headers);

        await _publisher.PublishAsync(message);

        Assert.Contains("x-contract-version", capturedHeaders!.Keys);
        Assert.Equal("2", capturedHeaders["x-contract-version"]);
    }

    [Fact]
    public async Task PublishAsync_WithoutMessageContract_DoesNotIncludeContractVersionHeader()
    {
        var message = new UncontractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        IDictionary<string, object>? capturedHeaders = null;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => capturedHeaders = headers);

        await _publisher.PublishAsync(message);

        Assert.DoesNotContain("x-contract-version", capturedHeaders!.Keys);
    }

    #endregion

    #region PublishAsync — Serializer & Transport Delegation

    [Fact]
    public async Task PublishAsync_PassesSerializedData_ToTransport()
    {
        var message = new ContractedEvent();
        var expectedData = new byte[] { 10, 20, 30 };
        _serializerMock.Setup(s => s.Serialize(message, typeof(ContractedEvent)))
            .Returns(expectedData);

        ReadOnlyMemory<byte> capturedData = default;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, data, _, _) => capturedData = data);

        await _publisher.PublishAsync(message);

        Assert.Equal(expectedData, capturedData.ToArray());
    }

    [Fact]
    public async Task PublishAsync_PassesCancellationToken_ToTransport()
    {
        using var cts = new CancellationTokenSource();
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        CancellationToken capturedToken = default;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, _, token) => capturedToken = token);

        await _publisher.PublishAsync(message, cts.Token);

        Assert.Equal(cts.Token, capturedToken);
    }

    [Fact]
    public async Task PublishAsync_PropagatesTransportException()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Transport unavailable"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _publisher.PublishAsync(message));
        Assert.Equal("Transport unavailable", ex.Message);
    }

    #endregion

    #region SendAsync — Delegates to PublishAsync

    [Fact]
    public async Task SendAsync_DelegatesToTransport_SameAsPublishAsync()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        await _publisher.SendAsync(message);

        _transportMock.Verify(
            t => t.PublishAsync(
                "sales.order.created",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_PassesCancellationToken_Through()
    {
        using var cts = new CancellationTokenSource();
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        CancellationToken capturedToken = default;
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, _, token) => capturedToken = token);

        await _publisher.SendAsync(message, cts.Token);

        Assert.Equal(cts.Token, capturedToken);
    }

    #endregion

    #region PublishAsync — Multiple Calls Produce Unique Message IDs

    [Fact]
    public async Task PublishAsync_MultipleCalls_ProduceUniqueMessageIds()
    {
        var message = new ContractedEvent();
        _serializerMock.Setup(s => s.Serialize(message, It.IsAny<Type>()))
            .Returns(new byte[] { 1 });

        var messageIds = new List<string>();
        _transportMock
            .Setup(t => t.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, IDictionary<string, object>, CancellationToken>(
                (_, _, headers, _) => messageIds.Add((string)headers["x-message-id"]));

        await _publisher.PublishAsync(message);
        await _publisher.PublishAsync(message);
        await _publisher.PublishAsync(message);

        Assert.Equal(3, messageIds.Count);
        Assert.Equal(3, messageIds.Distinct().Count());
    }

    #endregion

    #region Test Messages

    [MessageContract(Id = "sales.order.created", Version = "2", Kind = MessageKind.Event)]
    private sealed record ContractedEvent : IEvent;

    private sealed record UncontractedEvent : IEvent;

    #endregion
}
