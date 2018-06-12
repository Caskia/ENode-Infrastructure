using Confluent.Kafka;

namespace ENode.Kafka.Consumers
{
    public interface IMessageHandler<TKey, TValue>
    {
        void Handle(Message<TKey, TValue> message, IMessageContext<TKey, TValue> context);
    }
}