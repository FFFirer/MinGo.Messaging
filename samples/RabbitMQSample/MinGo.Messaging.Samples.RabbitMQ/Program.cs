using MinGo.Messaging;
using MinGo.Messaging.Samples.RabbitMQ;
using MinGo.Messaging.Samples.RabbitMQ.Consumers;

var builder = Host.CreateApplicationBuilder(args);

// Add messaging with auto-discovery
// AddPublishers() auto-scans assemblies for [MessageBus] interfaces and resolves transports from config
builder.Services.AddMessaging(builder.Configuration)
    .AddPublishers()
    .AddConsumer(typeof(OrderCreatedBillingHandler).Assembly);

// Add messaging hosted service for lifecycle management
builder.AddMessagingHost();

// Register the background publisher worker
builder.Services.AddHostedService<OrderPublisherWorker>();

var host = builder.Build();
host.Run();
