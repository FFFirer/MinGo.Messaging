using MinGo.Messaging;
using MinGo.Messaging.Samples.SimpleMessageBroker;
using MinGo.Messaging.Samples.SimpleMessageBroker.Consumers;
using MinGo.Messaging.SimpleMessageBroker;

var builder = Host.CreateApplicationBuilder(args);

// Add messaging with an EXPLICIT integration registration.
//
// UseSimpleMessageBroker() registers the SimpleMessageBroker transport, its keyed publisher, and the
// underlying Client SDK services directly (binding options from
// Messaging:Integrations:SimpleMessageBroker) — no assembly scanning is involved. This is the
// AOT/trimming-friendly path and claims integration discovery, so the implicit whole-graph scan won't
// also run; keep it before AddPublishers/AddConsumer.
//
// Publishers and consumers are still discovered by convention: the parameterless AddPublishers() scans
// the whole dependency graph for [MessageBus] interfaces, and AddConsumer(Assembly) is explicit.
builder.Services.AddMessaging(builder.Configuration)
    // Explicit integration: no reflection over the dependency graph to find the SimpleMessageBroker SDK.
    .UseSimpleMessageBroker()
    // Parameterless form scans the whole dependency graph for [MessageBus] interfaces.
    .AddPublishers()
    // Explicit-assembly form for consumers.
    .AddConsumer(typeof(OrderCreatedBillingHandler).Assembly);

// Add messaging hosted service for lifecycle management
builder.AddMessagingHost();

// Register the background publisher worker
builder.Services.AddHostedService<OrderPublisherWorker>();

var host = builder.Build();
host.Run();
