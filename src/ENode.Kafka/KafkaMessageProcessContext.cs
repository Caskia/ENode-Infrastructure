using Confluent.Kafka;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;

namespace ENode.Kafka
{
    public class KafkaMessageProcessContext : IMessageProcessContext
    {
        protected readonly Message<Ignore, string> _message;
        protected readonly IMessageContext<Ignore, string> _messageContext;

        public KafkaMessageProcessContext(Message<Ignore, string> message, IMessageContext<Ignore, string> messageContext)
        {
            _message = message;
            _messageContext = messageContext;
        }

        public virtual void NotifyMessageProcessed()
        {
            _messageContext.OnMessageHandled(_message);
        }
    }
}