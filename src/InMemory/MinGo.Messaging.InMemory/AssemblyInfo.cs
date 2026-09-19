using MinGo.Messaging.InMemory;
using MinGo.Messaging.Integration;
using MinGo.Messaging.Transport;

[assembly: MessagingIntegration(InMemoryIntegrationRegistration.IntegrationName, typeof(InMemoryMessagingTransport))]
