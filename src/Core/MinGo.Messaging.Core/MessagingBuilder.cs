using System.Reflection;
using MinGo.Messaging.Internal;
using MinGo.Messaging.Pipeline;
using MinGo.Messaging.Serialization;
using MinGo.Messaging.Subscriptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MinGo.Messaging;

/// <summary>
/// Default implementation of <see cref="IMessagingBuilder"/>.
/// </summary>
internal sealed class MessagingBuilder : IMessagingBuilder
{
    private readonly List<Assembly> _consumerAssemblies = new();
    private readonly List<string> _publisherNames = new();

    public MessagingBuilder(IServiceCollection services, IConfiguration configuration)
    {
        Services = services;
        Configuration = configuration;
    }

    public IServiceCollection Services { get; }

    public IConfiguration Configuration { get; }

    public IMessagingBuilder AddPublisher(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _publisherNames.Add(name);
        return this;
    }

    public IMessagingBuilder AddConsumer(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        _consumerAssemblies.Add(assembly);
        return this;
    }

    public IMessagingBuilder ConfigureSerializer(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        Services.AddSingleton<IMessageSerializer>(serializer);
        return this;
    }

    public IMessagingBuilder AddPipelineMiddleware<TMiddleware>() where TMiddleware : class, IConsumerPipelineMiddleware
    {
        Services.AddSingleton<IConsumerPipelineMiddleware, TMiddleware>();
        return this;
    }

    /// <summary>
    /// Gets the registered consumer assemblies for internal use.
    /// </summary>
    internal IReadOnlyList<Assembly> ConsumerAssemblies => _consumerAssemblies;

    /// <summary>
    /// Gets the registered publisher names for internal use.
    /// </summary>
    internal IReadOnlyList<string> PublisherNames => _publisherNames;
}
