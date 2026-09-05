using System.Reflection;
using MinGo.Messaging.Subscriptions;

namespace MinGo.Messaging.Integration;

/// <summary>
/// Builds <see cref="MessageSubscription"/> instances from <see cref="MessageConsumerAttribute"/> declarations.
/// </summary>
internal sealed class SubscriptionBuilder
{
    /// <summary>
    /// Scans an assembly for classes decorated with <see cref="MessageConsumerAttribute"/>
    /// and builds corresponding <see cref="MessageSubscription"/> instances.
    /// </summary>
    public IReadOnlyList<MessageSubscription> BuildSubscriptions(Assembly assembly, string? serviceId = null)
    {
        var subscriptions = new List<MessageSubscription>();
        var resolvedServiceId = serviceId ?? assembly.GetName().Name ?? "unknown-service";

        foreach (var type in assembly.GetExportedTypes())
        {
            var consumerAttrs = type.GetCustomAttributes<MessageConsumerAttribute>();

            foreach (var attr in consumerAttrs)
            {
                var contractAttr = attr.Contract.GetCustomAttribute<MessageContractAttribute>();
                var contractId = contractAttr?.Id ?? attr.Contract.FullName ?? attr.Contract.Name;
                var contractVersion = contractAttr?.Version ?? "1";

                var subscription = new MessageSubscription
                {
                    Id = $"sub-{resolvedServiceId}-{attr.Group}-{contractId}",
                    ContractId = contractId,
                    ContractVersion = contractVersion,
                    ConsumerServiceId = resolvedServiceId,
                    DeliveryMode = attr.DeliveryMode,
                    Target = new DeliveryTarget
                    {
                        Type = DeliveryTargetType.ConsumerGroup,
                        ConsumerGroupId = attr.Group
                    }
                };

                subscriptions.Add(subscription);
            }
        }

        return subscriptions;
    }
}
