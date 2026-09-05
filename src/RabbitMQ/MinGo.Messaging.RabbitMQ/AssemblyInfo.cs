using MinGo.Messaging.Integration;
using MinGo.Messaging.RabbitMQ;
using MinGo.Messaging.Transport;

[assembly: MessagingIntegration("RabbitMQ", typeof(RabbitMQMessagingTransport))]
