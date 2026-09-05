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
| SimpleMessageBroker | `MinGo.Messaging.SimpleMessageBroker` | SimpleMessageBroker transport (pull-based consumption, JSON over HTTP) |

## Key Concepts

- **Contract-First**: `[MessageContract]` attribute defines message identity
- **Subscription as First-Class Entity**: Business fan-out separated from runtime topology
- **Two-Layer Fan-out**: Event → Subscriptions (business) → Consumer Instances (scale)
- **DeliveryMode**: `Competing` (load balance) vs `Broadcast` (all instances)
- **Named Messaging Endpoints**: Publisher routing via Keyed DI
- **Explicit Integration Registration**: `UseRabbitMQ()` / `UseSimpleMessageBroker()` wire a known integration directly — no assembly scanning, AOT/trimming-friendly
- **Convention-Driven Auto-Assembly**: `[MessagingIntegration]` for zero-config integration discovery
- **Filter-Based Assembly Scanning**: discover integrations/consumers/publishers across the whole dependency graph, narrowed by `AssemblyName` filters

## Quick Start

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMessaging(builder.Configuration)
    // Register the integration explicitly — no assembly scanning required.
    .UseRabbitMQ()
    // Publishers and consumers are still discovered by convention.
    .AddPublishers()
    .AddConsumer(typeof(Program).Assembly);

builder.AddMessagingHost();

var host = builder.Build();
host.Run();
```

## Registering Integrations

An integration provides a transport (and its keyed `IMessagePublisher`). Register one either
**explicitly** or by **scanning**; both paths bind the same options from
`Messaging:Integrations:{Name}` and are behaviourally identical.

**Explicit (recommended — AOT/trimming-friendly, no reflection over the dependency graph):**

```csharp
builder.Services.AddMessaging(configuration)
    .UseRabbitMQ()                 // or .UseSimpleMessageBroker()
    .AddPublishers()
    .AddConsumer(typeof(Program).Assembly);
```

Each integration SDK ships a `Use{X}()` extension. Under the hood it invokes the SDK's own
`Configure` hook (binding options / registering client services) and then the Core primitive
`AddIntegration<TTransport>(name)`, which registers only the keyed transport + publisher. Call that
primitive directly for a custom transport:

```csharp
.AddIntegration<MyCustomTransport>("MyIntegration")
```

**Scanning (convention-driven, zero wiring):** `AddIntegrations` finds every assembly in the
dependency graph carrying `[MessagingIntegration]`, optionally narrowed by a filter:

```csharp
builder.Services.AddMessaging(configuration)
    .AddIntegrations(AssemblyFilters.NamePrefix("MinGo.Messaging."))
    .AddPublishers()
    .AddConsumer(typeof(Program).Assembly);
```

> Register the integration **before** `AddPublishers` / `AddConsumer` so their transports are
> available. Any explicit registration (or `AddIntegrations(...)`) claims integration discovery, so
> the implicit whole-graph scan will not also run.

## Assembly Scanning

Discovery walks the **entire application dependency graph** (`DependencyContext`), so assemblies
that are referenced but not yet loaded are still found. This fixes the classic problem where a
consumer or typed publisher declared in a referenced library is never registered because .NET loads
assemblies lazily.

Each `AddIntegrations` / `AddPublishers` / `AddConsumer` accepts an optional
`Func<AssemblyName, bool>` **filter**:

- Filters are evaluated against the `AssemblyName` **before loading**, so only matching assemblies
  are loaded and inspected — no more loading the entire dependency closure.
- No filter → the whole dependency graph is scanned (the broadest, backward-compatible default).
- Repeated calls combine as a union and results are de-duplicated.
- `AssemblyFilters` ships ready-made predicates: `NamePrefix`, `NameEquals`, `NameContains`,
  `NameMatches`. A plain `name => ...` lambda works just as well.
- The parameterless `AddPublishers()` and explicit `AddConsumer(Assembly)` forms remain supported.

```csharp
builder.Services.AddMessaging(configuration)
    .AddIntegrations(name => name.Name!.StartsWith("MinGo.Messaging."))
    .AddConsumer(typeof(MyHandler).Assembly);   // explicit assembly still works
```

## License

MIT
