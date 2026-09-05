using System.Reflection;

namespace MinGo.Messaging.Integration;

/// <summary>
/// Discovers Integration SDKs by scanning loaded assemblies for <see cref="MessagingIntegrationAttribute"/>.
/// </summary>
internal sealed class IntegrationDiscovery
{
    /// <summary>
    /// Scans all loaded assemblies for Integration SDK declarations.
    /// </summary>
    public IReadOnlyList<IntegrationDescriptor> DiscoverIntegrations()
    {
        var integrations = new List<IntegrationDescriptor>();

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var attr = assembly.GetCustomAttribute<MessagingIntegrationAttribute>();
                if (attr is not null)
                {
                    integrations.Add(new IntegrationDescriptor(attr.Name, attr.TransportType, assembly));
                }
            }
            catch
            {
                // Skip assemblies that cannot be inspected (dynamic, reflection-only, etc.)
            }
        }

        return integrations;
    }
}
