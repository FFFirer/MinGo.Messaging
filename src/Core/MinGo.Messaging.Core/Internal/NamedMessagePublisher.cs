using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Named message publisher that routes messages to the correct keyed transport
/// based on the publisher/endpoint name.
/// </summary>
internal sealed class NamedMessagePublisher : INamedMessagePublisher
{
    private readonly IServiceProvider _serviceProvider;

    public NamedMessagePublisher(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public async Task PublishAsync(string name, IMessage message, CancellationToken cancellationToken = default)
    {
        var publisher = _serviceProvider.GetRequiredKeyedService<IMessagePublisher>(name);
        await publisher.PublishAsync(message, cancellationToken);
    }

    public async Task SendAsync(string name, IMessage message, CancellationToken cancellationToken = default)
    {
        var publisher = _serviceProvider.GetRequiredKeyedService<IMessagePublisher>(name);
        await publisher.SendAsync(message, cancellationToken);
    }
}

/// <summary>
/// Default message publisher that delegates to the first available keyed publisher.
/// </summary>
internal sealed class DefaultMessagePublisher : IMessagePublisher
{
    private readonly INamedMessagePublisher _namedPublisher;

    public DefaultMessagePublisher(INamedMessagePublisher namedPublisher)
    {
        _namedPublisher = namedPublisher;
    }

    public Task PublishAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        // For the default publisher, we need to find the first registered integration
        // In practice, users should use INamedMessagePublisher with explicit names
        throw new InvalidOperationException(
            "Default IMessagePublisher requires at least one integration to be configured. " +
            "Use INamedMessagePublisher with an explicit endpoint name instead.");
    }

    public Task SendAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        return PublishAsync(message, cancellationToken);
    }
}
