using System.Reflection;
using System.Text.RegularExpressions;

namespace MinGo.Messaging;

/// <summary>
/// Factory methods for common <see cref="AssemblyName"/> filters used to select which assemblies
/// are scanned from the application dependency graph.
/// </summary>
/// <remarks>
/// Filters are evaluated against an <see cref="AssemblyName"/> <em>before</em> the assembly is
/// loaded. For assemblies that are not loaded yet, only <see cref="AssemblyName.Name"/> is
/// populated (version/culture/public-key are absent), so prefer name-based predicates.
/// </remarks>
/// <example>
/// <code>
/// builder.Services.AddMessaging(builder.Configuration)
///     .AddIntegrations(AssemblyFilters.NamePrefix("MinGo.Messaging."))
///     .AddPublishers(AssemblyFilters.NamePrefix("Acme."))
///     .AddConsumer(AssemblyFilters.NameMatches(@"^Acme\..*\.Handlers$"));
/// </code>
/// </example>
public static class AssemblyFilters
{
    /// <summary>
    /// Matches assemblies whose simple name starts with <paramref name="prefix"/> (ordinal, case-insensitive).
    /// </summary>
    public static Func<AssemblyName, bool> NamePrefix(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        return name => name.Name is not null
            && name.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Matches assemblies whose simple name equals <paramref name="assemblyName"/> (ordinal, case-insensitive).
    /// </summary>
    public static Func<AssemblyName, bool> NameEquals(string assemblyName)
    {
        ArgumentException.ThrowIfNullOrEmpty(assemblyName);
        return name => string.Equals(name.Name, assemblyName, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Matches assemblies whose simple name contains <paramref name="substring"/> (ordinal, case-insensitive).
    /// </summary>
    public static Func<AssemblyName, bool> NameContains(string substring)
    {
        ArgumentException.ThrowIfNullOrEmpty(substring);
        return name => name.Name is not null
            && name.Name.Contains(substring, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Matches assemblies whose simple name matches the supplied regular expression <paramref name="pattern"/>.
    /// </summary>
    public static Func<AssemblyName, bool> NameMatches(string pattern)
    {
        ArgumentException.ThrowIfNullOrEmpty(pattern);
        var regex = new Regex(pattern, RegexOptions.Compiled | RegexOptions.CultureInvariant);
        return name => name.Name is not null && regex.IsMatch(name.Name);
    }
}
