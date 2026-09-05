using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MinGo.Messaging;

/// <summary>
/// Extension methods for adding messaging host services.
/// </summary>
public static class MessagingHostBuilderExtensions
{
    /// <summary>
    /// Adds the messaging hosted service to the application.
    /// This manages the transport lifecycle (connect/disconnect) and consumer subscription.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <returns>The builder for chaining.</returns>
    public static IHostApplicationBuilder AddMessagingHost(this IHostApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddHostedService<MessagingHostedService>();

        return builder;
    }
}
