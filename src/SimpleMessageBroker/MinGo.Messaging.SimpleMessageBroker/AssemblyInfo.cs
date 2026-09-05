using MinGo.Messaging.Integration;
using MinGo.Messaging.SimpleMessageBroker;
using MinGo.Messaging.Transport;

[assembly: MessagingIntegration("SimpleMessageBroker", typeof(SimpleMessageBrokerMessagingTransport))]
