using System.Reflection;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Dynamic proxy that implements a typed message bus publisher interface
/// (e.g. IOrderBusPublisher) by delegating to the underlying keyed <see cref="IMessagePublisher"/>.
/// </summary>
internal sealed class TypedMessagePublisher : DispatchProxy
{
    private IMessagePublisher _inner = null!;

    /// <summary>
    /// Sets the underlying publisher this proxy delegates to.
    /// Called immediately after proxy creation.
    /// </summary>
    internal void Initialize(IMessagePublisher inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        return targetMethod!.Invoke(_inner, args);
    }
}
