using MinGo.Messaging;
using MinGo.Messaging.RabbitMQ;
using MinGo.Messaging.Samples.RabbitMQ;

var builder = Host.CreateApplicationBuilder(args);

// Add messaging with an EXPLICIT integration registration.
//
// UseRabbitMQ() registers the RabbitMQ transport + keyed publisher directly (binding options from
// Messaging:Integrations:RabbitMQ) — no assembly scanning is involved. This is the AOT/trimming-
// friendly path and the recommended way to wire a known integration. It claims integration discovery,
// so the implicit whole-graph scan won't also run; keep it before AddPublishers/AddConsumer.
//
// Publishers and consumers are still discovered by convention, narrowed by AssemblyName filters over
// the dependency graph (evaluated BEFORE load, so only matching assemblies are loaded).
builder.Services.AddMessaging(builder.Configuration)
    // Explicit integration: no reflection over the dependency graph to find the RabbitMQ SDK.
    .UseRabbitMQ()
    // Typed publishers ([MessageBus]) are declared in the sample assembly.
    .AddPublishers(name => name.Name!.StartsWith("MinGo.Messaging.Samples.", StringComparison.OrdinalIgnoreCase))
    // Consumers ([MessageConsumer]) are declared in the sample assembly too.
    .AddConsumer(AssemblyFilters.NamePrefix("MinGo.Messaging.Samples."));

// Add messaging hosted service for lifecycle management
builder.AddMessagingHost();

// Register the background publisher worker
builder.Services.AddHostedService<OrderPublisherWorker>();

var host = builder.Build();
host.Run();
