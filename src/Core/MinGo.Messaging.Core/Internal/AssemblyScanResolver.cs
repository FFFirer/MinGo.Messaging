using System.Reflection;
using Microsoft.Extensions.DependencyModel;

namespace MinGo.Messaging.Internal;

/// <summary>
/// Resolves the set of assemblies that messaging conventions are scanned from.
/// </summary>
/// <remarks>
/// Candidate assemblies come from two sources:
/// <list type="number">
///   <item>The assemblies already loaded in the current <see cref="AppDomain"/> (fast path).</item>
///   <item>The full application dependency graph (<see cref="DependencyContext"/>), which surfaces
///   assemblies that are referenced but not yet loaded. This is essential because .NET loads
///   assemblies lazily — an Integration SDK or a handlers library referenced via ProjectReference
///   may not be loaded yet if no startup code directly touches one of its types.</item>
/// </list>
/// User-supplied filters are evaluated against the <see cref="AssemblyName"/> <em>before</em> any
/// assembly is loaded, so only matching assemblies are actually loaded and inspected. When no filter
/// is supplied, every candidate is included (the broadest, backward-compatible default).
/// </remarks>
internal sealed class AssemblyScanResolver
{
    private static readonly Func<AssemblyName, bool>[] NoFilters = [];

    private readonly object _gate = new();

    // Simple assembly name -> loaded instance, populated as candidates are discovered/loaded.
    private Dictionary<string, Assembly>? _loadedByName;

    // Deduplicated candidate names (loaded assemblies + dependency graph), computed once.
    private List<AssemblyName>? _candidates;

    // Cache for the no-filter case so repeated default scans do not re-walk/re-load.
    private IReadOnlyList<Assembly>? _allResolved;

    /// <summary>
    /// Resolves all assemblies matching any of the supplied filters. An empty filter set
    /// matches every candidate assembly.
    /// </summary>
    public IReadOnlyList<Assembly> Resolve(IReadOnlyList<Func<AssemblyName, bool>> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);

        if (filters.Count == 0)
        {
            return _allResolved ??= LoadMatching(static _ => true);
        }

        return LoadMatching(name =>
        {
            foreach (var filter in filters)
            {
                if (filter(name)) return true;
            }

            return false;
        });
    }

    /// <summary>
    /// Resolves all assemblies matching the supplied filter.
    /// </summary>
    public IReadOnlyList<Assembly> Resolve(Func<AssemblyName, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Resolve([filter]);
    }

    /// <summary>
    /// Resolves every candidate assembly (no filtering).
    /// </summary>
    public IReadOnlyList<Assembly> ResolveAll() => Resolve(NoFilters);

    private IReadOnlyList<Assembly> LoadMatching(Func<AssemblyName, bool> predicate)
    {
        var result = new List<Assembly>();

        foreach (var candidate in GetCandidates())
        {
            if (!predicate(candidate)) continue;

            var assembly = TryLoad(candidate);
            if (assembly is not null) result.Add(assembly);
        }

        return result;
    }

    private IReadOnlyList<AssemblyName> GetCandidates()
    {
        if (_candidates is not null) return _candidates;

        lock (_gate)
        {
            if (_candidates is not null) return _candidates;

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var candidates = new List<AssemblyName>();

            // 1. Already-loaded assemblies (full AssemblyName available, no I/O).
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var name = assembly.GetName();
                if (name.Name is null || !seen.Add(name.Name)) continue;

                candidates.Add(name);
                TrackLoaded(name.Name, assembly);
            }

            // 2. Application dependency graph — surfaces referenced-but-not-yet-loaded assemblies.
            var dependencyContext = DependencyContext.Default;
            if (dependencyContext is not null)
            {
                foreach (var library in dependencyContext.RuntimeLibraries)
                {
                    // Only project/package libraries carry scannable managed assemblies; skip
                    // runtime packs (shared framework) to avoid needless load attempts.
                    if (!IsScannableLibrary(library)) continue;

                    foreach (var simpleName in EnumerateAssemblyNames(library))
                    {
                        if (!seen.Add(simpleName)) continue;

                        // Only the simple name is known before loading; version/culture are absent.
                        candidates.Add(new AssemblyName(simpleName));
                    }
                }
            }

            _candidates = candidates;
            return _candidates;
        }
    }

    private Assembly? TryLoad(AssemblyName candidate)
    {
        var simpleName = candidate.Name!;

        if (_loadedByName is not null && _loadedByName.TryGetValue(simpleName, out var loaded))
        {
            return loaded;
        }

        try
        {
            var assembly = Assembly.Load(simpleName);
            TrackLoaded(simpleName, assembly);
            return assembly;
        }
        catch
        {
            // Skip assemblies that cannot be loaded (native, missing, reflection-only, etc.).
            return null;
        }
    }

    private void TrackLoaded(string simpleName, Assembly assembly)
    {
        _loadedByName ??= new Dictionary<string, Assembly>(StringComparer.OrdinalIgnoreCase);
        _loadedByName[simpleName] = assembly;
    }

    private static bool IsScannableLibrary(RuntimeLibrary library)
        => library.Type is "project" or "package";

    private static IReadOnlyList<string> EnumerateAssemblyNames(RuntimeLibrary library)
    {
        var names = new List<string>();

        foreach (var group in library.RuntimeAssemblyGroups)
        {
            foreach (var assetPath in group.AssetPaths)
            {
                // deps.json paths always use '/' separators (e.g. "lib/net8.0/Foo.dll").
                var simpleName = Path.GetFileNameWithoutExtension(assetPath);
                if (!string.IsNullOrWhiteSpace(simpleName)) names.Add(simpleName);
            }
        }

        // Fall back to the library name when no runtime assemblies are declared.
        if (names.Count == 0 && !string.IsNullOrWhiteSpace(library.Name)) names.Add(library.Name);

        return names;
    }
}
