using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging.Subscriptions;

/// <summary>
/// Registry that holds all discovered message subscriptions.
/// </summary>
public sealed class SubscriptionRegistry
{
    private readonly List<MessageSubscription> _subscriptions = new();

    /// <summary>
    /// Gets all registered subscriptions.
    /// </summary>
    public IReadOnlyList<MessageSubscription> Subscriptions => _subscriptions;

    /// <summary>
    /// Registers a subscription.
    /// </summary>
    internal void Register(MessageSubscription subscription)
    {
        _subscriptions.Add(subscription);
    }

    /// <summary>
    /// Registers multiple subscriptions.
    /// </summary>
    internal void RegisterRange(IEnumerable<MessageSubscription> subscriptions)
    {
        _subscriptions.AddRange(subscriptions);
    }

    /// <summary>
    /// Finds all subscriptions for a given contract identifier (fan-out query).
    /// </summary>
    public IReadOnlyList<MessageSubscription> FindByContract(string contractId)
    {
        return _subscriptions.Where(s => s.ContractId == contractId).ToList();
    }

    /// <summary>
    /// Finds a subscription by its identifier.
    /// </summary>
    public MessageSubscription? FindById(string subscriptionId)
    {
        return _subscriptions.FirstOrDefault(s => s.Id == subscriptionId);
    }
}
