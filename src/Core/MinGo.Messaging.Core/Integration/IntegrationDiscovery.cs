using System.Reflection;

namespace MinGo.Messaging.Integration;

/// <summary>
/// Reads <see cref="MessagingIntegrationAttribute"/> declarations from a set of assemblies.
/// </summary>
/// <remarks>
/// Assembly selection (which assemblies to inspect) is the responsibility of
/// <c>AssemblyScanResolver</c>; this type only inspects the assemblies it is given.
/// </remarks>
internal static class IntegrationDiscovery
{
    /// <summary>
    /// Returns one <see cref="IntegrationDescriptor"/> per assembly that carries a
    /// <see cref="MessagingIntegrationAttribute"/>. Assemblies that cannot be inspected are skipped.
    /// </summary>
    public static IReadOnlyList<IntegrationDescriptor> FindIntegrations(IEnumerable<Assembly> assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var integrations = new List<IntegrationDescriptor>();

        foreach (var assembly in assemblies)
        {
            TryAddIntegration(assembly, integrations);
        }

        return integrations;
    }

    private static void TryAddIntegration(Assembly assembly, List<IntegrationDescriptor> integrations)
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
}
