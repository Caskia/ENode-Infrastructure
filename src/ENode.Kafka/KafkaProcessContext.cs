using Confluent.Kafka;
using ENode.Infrastructure;

namespace ENode.Kafka
{
    public class KafkaProcessContext : IMessageProcessContext
    {
        protected readonly Consumer<Ignore, string> _consumer;
        protected readonly Message<Ignore, string> _message;

        public KafkaProcessContext(Consumer<Ignore, string> consumer, Message<Ignore, string> message)
        {
            _consumer = consumer;
            _message = message;
        }

        public virtual void NotifyMessageProcessed()
        {
            _consumer.CommitAsync(_message);
        }
    }
}