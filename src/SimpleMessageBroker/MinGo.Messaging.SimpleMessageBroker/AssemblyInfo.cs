using MinGo.Messaging.Integration;
using MinGo.Messaging.SimpleMessageBroker;
using MinGo.Messaging.Transport;

[assembly: MessagingIntegration(SimpleMessageBrokerIntegrationRegistration.IntegrationName, typeof(SimpleMessageBrokerMessagingTransport))]
