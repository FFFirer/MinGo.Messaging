# MinGo.Messaging

A business-oriented, runtime-middleware-agnostic messaging SDK for .NET 8+.

## Architecture

Three-layer design: **Base SDK** → **Integration SDK** → **Middleware Client**

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Abstractions | `MinGo.Messaging.Abstractions` | Contract models, SPI interfaces, Subscription domain model |
| Core | `MinGo.Messaging.Core` | Auto-discovery, Keyed DI, consumer pipeline, named publishers |
| Hosting | `MinGo.Messaging.Hosting` | IHostedService lifecycle management |
| RabbitMQ | `MinGo.Messaging.RabbitMQ` | RabbitMQ transport with Competing/Broadcast delivery modes |

## Key Concepts

- **Contract-First**: `[MessageContract]` attribute defines message identity
- **Subscription as First-Class Entity**: Business fan-out separated from runtime topology
- **Two-Layer Fan-out**: Event → Subscriptions (business) → Consumer Instances (scale)
- **DeliveryMode**: `Competing` (load balance) vs `Broadcast` (all instances)
- **Named Messaging Endpoints**: Publisher routing via Keyed DI
- **Convention-Driven Auto-Assembly**: `[MessagingIntegration]` for zero-config integration

## Quick Start

```csharp
builder.Services
    .AddMessaging()
    .AddPublisher("OrderBus")
    .AddConsumer(typeof(Program).Assembly)
    .AddMessagingHost();
```

## License

MIT
